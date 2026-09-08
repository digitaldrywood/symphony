package hubserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const (
	defaultHTTPReadHeaderTimeout = 5 * time.Second
	defaultHTTPIdleTimeout       = 2 * time.Minute
)

type Service struct {
	billing           *hostedBillingWorker
	hostedMutationMu  sync.Mutex
	hostedSessions    *auth.Service
	echo              *echo.Echo
	database          *database
	tracker           tracker.Tracker
	config            Config
	outbox            *outboxWorker
	ready             atomic.Bool
	workerCancel      context.CancelFunc
	workerDone        chan struct{}
	workerStopOnce    sync.Once
	reconcileCancel   context.CancelFunc
	reconcileDone     chan struct{}
	reconcileStopOnce sync.Once
	closeOnce         sync.Once
	closeErr          error
}

type healthResponse struct {
	Status        string                  `json:"status"`
	SchemaVersion int64                   `json:"schema_version,omitempty"`
	Version       string                  `json:"version,omitempty"`
	Outbox        OutboxHealth            `json:"outbox"`
	Repositories  RepositoryHealthSummary `json:"repositories"`
}

func Open(ctx context.Context, cfg Config) (*Service, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg = cfg.normalized()
	if err := cfg.Hosted.validate(); err != nil {
		return nil, err
	}
	if cfg.Hosted != nil {
		cfg.Logger = slog.New(hostedLogHandler{output: cfg.Logger.Handler()})
	}
	database, err := openDatabase(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := database.ensureInitialAdminToken(ctx, cfg.InitialAdminToken); err != nil {
		return nil, errors.Join(err, database.Close())
	}
	cfg.InitialAdminToken = nil
	workTracker, err := tracker.NewStore(database)
	if err != nil {
		return nil, errors.Join(err, database.Close())
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Server.ReadHeaderTimeout = defaultHTTPReadHeaderTimeout
	e.Server.IdleTimeout = defaultHTTPIdleTimeout
	workerContext, workerCancel := context.WithCancel(context.Background())
	reconcileContext, reconcileCancel := context.WithCancel(context.Background())
	service := &Service{
		echo:            e,
		database:        database,
		tracker:         workTracker,
		config:          cfg,
		workerCancel:    workerCancel,
		workerDone:      make(chan struct{}),
		reconcileCancel: reconcileCancel,
		reconcileDone:   make(chan struct{}),
	}
	if cfg.OutboxBackend != nil {
		service.outbox = newOutboxWorker(service)
	}
	if cfg.Hosted != nil {
		service.hostedSessions, err = auth.NewSessionService(auth.SessionConfig{SessionTTL: 30 * 24 * time.Hour, PublicURL: cfg.Hosted.PublicURL}, service)
		if err != nil {
			workerCancel()
			reconcileCancel()
			return nil, errors.Join(err, database.Close())
		}
	}
	service.registerRoutes(e)
	service.maintainGitHubWebhooks(ctx)
	go service.runGitHubWebhookMaintenance(workerContext)
	go service.runGitHubReconciliation(reconcileContext)
	service.ready.Store(true)
	if service.outbox != nil {
		service.outbox.start(ctx)
	}
	service.startHostedBilling()
	return service, nil
}

func (s *Service) Tracker() tracker.Tracker {
	return s.tracker
}

func Run(ctx context.Context, cfg Config) (resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cfg = cfg.normalized()
	if err := validateListenerSecurity(cfg); err != nil {
		return err
	}
	service, err := Open(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, service.Close())
	}()

	listener, err := cfg.listen(ctx, "tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen for hub requests: %w", err)
	}
	cfg.Logger.Info("hub serving", "address", listener.Addr().String())

	serveResult := make(chan error, 1)
	go func() {
		if cfg.TLSCertFile != "" {
			serveResult <- service.ServeTLS(listener, cfg.TLSCertFile, cfg.TLSKeyFile)
			return
		}
		serveResult <- service.Serve(listener)
	}()

	select {
	case err := <-serveResult:
		return err
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		shutdownErr := service.Shutdown(shutdownContext)
		cancel()
		if shutdownErr != nil {
			shutdownErr = errors.Join(shutdownErr, service.Close())
		}
		return errors.Join(shutdownErr, <-serveResult)
	}
}

func validateListenerSecurity(cfg Config) error {
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if (certFile == "") != (keyFile == "") {
		return errors.New("hub TLS certificate and key must be configured together")
	}
	if listenerAddressLoopback(cfg.ListenAddress) || certFile != "" || cfg.TrustedProxy {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrInsecureListener, cfg.ListenAddress)
}

func listenerAddressLoopback(address string) bool {
	host, _, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil {
		return false
	}
	host = strings.Trim(strings.TrimSpace(host), "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Service) Handler() http.Handler {
	return s.echo
}

func (s *Service) Serve(listener net.Listener) error {
	if listener == nil {
		return errors.New("hub listener is required")
	}
	err := s.echo.Server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("serve hub requests: %w", err)
	}
	return nil
}

func (s *Service) ServeTLS(listener net.Listener, certificateFile string, keyFile string) error {
	if listener == nil {
		return errors.New("hub listener is required")
	}
	err := s.echo.Server.ServeTLS(listener, strings.TrimSpace(certificateFile), strings.TrimSpace(keyFile))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("serve TLS hub requests: %w", err)
	}
	return nil
}

func (s *Service) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.ready.Store(false)
	httpErr := s.echo.Shutdown(ctx)
	if errors.Is(httpErr, http.ErrServerClosed) {
		httpErr = nil
	}
	if httpErr != nil {
		httpErr = fmt.Errorf("shut down hub server: %w", httpErr)
	}
	s.stopHostedBilling()
	return errors.Join(httpErr, s.stopGitHubWebhookMaintenance(), s.stopGitHubReconciliation())
}

func (s *Service) Backup(ctx context.Context, destination string) error {
	if !s.ready.Load() {
		return ErrNotReady
	}
	return s.database.backup(ctx, destination)
}

func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		s.ready.Store(false)
		httpErr := s.echo.Close()
		if errors.Is(httpErr, http.ErrServerClosed) {
			httpErr = nil
		}
		webhookErr := s.stopGitHubWebhookMaintenance()
		reconcileErr := s.stopGitHubReconciliation()
		s.stopHostedBilling()
		if s.outbox != nil {
			s.outbox.stop()
		}
		s.closeErr = errors.Join(httpErr, webhookErr, reconcileErr, s.database.Close())
	})
	return s.closeErr
}

func (s *Service) runGitHubWebhookMaintenance(ctx context.Context) {
	defer close(s.workerDone)
	ticker := time.NewTicker(s.config.WebhookMaintenanceInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.maintainGitHubWebhooks(ctx)
		}
	}
}

func (s *Service) maintainGitHubWebhooks(ctx context.Context) {
	now := s.config.now().UTC()
	if err := s.database.processPendingWebhooks(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
		s.config.Logger.Error("process pending GitHub webhooks", "error", err)
	}
	if _, err := s.database.purgeWebhookPayloads(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
		s.config.Logger.Warn("purge GitHub webhook payloads", "error", err)
	}
}

func (s *Service) stopGitHubWebhookMaintenance() error {
	if s == nil || s.workerCancel == nil || s.workerDone == nil {
		return nil
	}
	s.workerStopOnce.Do(func() {
		s.workerCancel()
		<-s.workerDone
	})
	return nil
}

func (s *Service) health(c echo.Context) error {
	if !s.ready.Load() || s.database.health(c.Request().Context()) != nil {
		return c.JSON(http.StatusServiceUnavailable, healthResponse{Status: "unavailable"})
	}
	outbox, err := s.OutboxHealth(c.Request().Context())
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, healthResponse{Status: "unavailable"})
	}
	repositories, err := s.database.repositoryFreshness(c.Request().Context(), s.config.now().UTC(), s.config.ReconcileInterval)
	if err != nil {
		return c.JSON(http.StatusServiceUnavailable, healthResponse{Status: "unavailable"})
	}
	status := "ok"
	if repositories.Summary.Stale > 0 || repositories.Summary.Error > 0 {
		status = "degraded"
	}
	return c.JSON(http.StatusOK, healthResponse{
		Status:        status,
		SchemaVersion: s.database.schemaVersion,
		Version:       s.config.Version,
		Outbox:        outbox,
		Repositories:  repositories.Summary,
	})
}
