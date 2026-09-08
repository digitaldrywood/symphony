package hubserver

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/billing"
)

type hostedBillingWorker struct {
	service *Service
	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan struct{}
}

func (s *Service) startHostedBilling() {
	if s.config.Hosted == nil || s.config.Hosted.Billing == nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.billing = &hostedBillingWorker{service: s, cancel: cancel, done: make(chan struct{})}
	go s.billing.run(ctx)
}

func (s *Service) stopHostedBilling() {
	if s.billing != nil {
		s.billing.cancel()
		<-s.billing.done
	}
}

func (w *hostedBillingWorker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(time.Duration(w.service.config.Hosted.Billing.ReconcileSeconds) * time.Second)
	defer ticker.Stop()
	for {
		w.mu.Lock()
		err := w.reconcile(ctx)
		w.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			w.service.config.Logger.Warn("hosted billing reconciliation unavailable; retaining bounded local access")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *hostedBillingWorker) reconcile(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	s := w.service
	var through int64
	if err := s.database.db.QueryRowContext(ctx, "SELECT coalesce(max(sequence),0) FROM hosted_billing_events").Scan(&through); err != nil {
		return err
	}
	previous, err := s.database.readHostedBilling(ctx)
	if err != nil {
		return err
	}
	cfg := s.config.Hosted.Billing
	snapshot, err := cfg.Provider.Reconcile(ctx, cfg.binding(s.config.Hosted.OrganizationID))
	if err != nil {
		return err
	}
	now := s.config.now().UTC()
	return s.database.commitHostedBilling(ctx, resolveHostedBilling(cfg, previous, snapshot, now), through, now)
}

func (s *Service) hostedStripeWebhook(c echo.Context) error {
	if s.config.Hosted.Billing == nil {
		return c.NoContent(http.StatusNotFound)
	}
	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, 1024*1024))
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	event, err := billing.VerifyEvent(body, c.Request().Header.Get("Stripe-Signature"), s.config.Hosted.Billing.WebhookSecret, s.config.now())
	if err != nil {
		return c.NoContent(http.StatusBadRequest)
	}
	if !event.Relevant() {
		return c.NoContent(http.StatusOK)
	}
	if _, err := s.database.db.ExecContext(c.Request().Context(), "INSERT INTO hosted_billing_events(event_id,event_type,received_at) VALUES(?,?,?) ON CONFLICT(event_id) DO NOTHING", event.ID, event.Type, formatHubTime(s.config.now())); err != nil {
		return c.NoContent(http.StatusServiceUnavailable)
	}
	return c.NoContent(http.StatusOK)
}
