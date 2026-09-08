package hubserver

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"time"

	"github.com/google/uuid"

	"github.com/digitaldrywood/detent/internal/apikey"
)

const (
	DefaultListenAddress              = "127.0.0.1:7777"
	DefaultWebhookPayloadRetention    = 7 * 24 * time.Hour
	DefaultWebhookMaintenanceInterval = time.Minute
	defaultBusyTimeout                = 5 * time.Second
	defaultShutdownTime               = 5 * time.Second
	defaultOutboxPoll                 = time.Second
	defaultOutboxBase                 = 2 * time.Second
	defaultOutboxMax                  = 15 * time.Minute
	defaultOutboxStale                = 5 * time.Minute
	defaultOutboxAttempts             = 8
	DefaultReconcileInterval          = 10 * time.Minute
	DefaultFullRepairInterval         = 24 * time.Hour
)

var (
	ErrBackupSource            = errors.New("hub backup destination matches the source database")
	ErrDatabaseIdentity        = errors.New("database is not a Detent Hub database")
	ErrInvalidClock            = errors.New("hub clock returned a zero time")
	ErrInsecureListener        = errors.New("non-loopback hub listener requires TLS or a trusted reverse proxy")
	ErrNetworkFilesystem       = errors.New("hub database requires a local filesystem")
	ErrNotReady                = errors.New("hub service is not ready")
	ErrOutboxDisabled          = errors.New("hub github outbox is disabled")
	ErrUnsupportedSchema       = errors.New("hub database schema is newer than this Detent version")
	ErrWebhookDeliveryConflict = errors.New("github webhook delivery ID has conflicting content")
)

type Config struct {
	CredentialMaintenance      bool
	Hosted                     *HostedConfig
	GitHubRequestCounts        func() []GitHubRequestCount
	GitHubDisabled             bool
	ImportBackend              ImportBackend
	DatabasePath               string
	ListenAddress              string
	TLSCertFile                string
	TLSKeyFile                 string
	TrustedProxy               bool
	InitialAdminToken          []byte
	BusyTimeout                time.Duration
	ShutdownTimeout            time.Duration
	GitHubWebhookSecret        []byte
	WebhookPayloadRetention    time.Duration
	WebhookMaintenanceInterval time.Duration
	Logger                     *slog.Logger
	Version                    string
	OutboxBackend              OutboxBackend
	OutboxPollInterval         time.Duration
	OutboxBaseBackoff          time.Duration
	OutboxMaxBackoff           time.Duration
	OutboxProcessingTimeout    time.Duration
	OutboxMaxAttempts          int
	ReconcileBackend           ReconcileBackend
	ReconcileInterval          time.Duration
	FullRepairInterval         time.Duration

	validateDatabaseFilesystem func(string) error
	listen                     func(context.Context, string, string) (net.Listener, error)
	now                        func() time.Time
	jitter                     func() float64
	newLeaseID                 func() string
	newTokenID                 func() string
	generateToken              func() (string, error)
}

func (c Config) normalized() Config {
	if c.CredentialMaintenance {
		c.GitHubDisabled = true
	}
	if c.GitHubDisabled {
		c.ImportBackend, c.OutboxBackend, c.ReconcileBackend = nil, nil, nil
		c.GitHubWebhookSecret, c.GitHubRequestCounts = nil, nil
	}
	if c.ListenAddress == "" {
		c.ListenAddress = DefaultListenAddress
	}
	if c.BusyTimeout <= 0 {
		c.BusyTimeout = defaultBusyTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = defaultShutdownTime
	}
	if c.WebhookPayloadRetention <= 0 {
		c.WebhookPayloadRetention = DefaultWebhookPayloadRetention
	}
	if c.WebhookMaintenanceInterval <= 0 {
		c.WebhookMaintenanceInterval = DefaultWebhookMaintenanceInterval
	}
	if c.OutboxPollInterval <= 0 {
		c.OutboxPollInterval = defaultOutboxPoll
	}
	if c.OutboxBaseBackoff <= 0 {
		c.OutboxBaseBackoff = defaultOutboxBase
	}
	if c.OutboxMaxBackoff <= 0 {
		c.OutboxMaxBackoff = defaultOutboxMax
	}
	if c.OutboxMaxBackoff < c.OutboxBaseBackoff {
		c.OutboxMaxBackoff = c.OutboxBaseBackoff
	}
	if c.OutboxProcessingTimeout <= 0 {
		c.OutboxProcessingTimeout = defaultOutboxStale
	}
	if c.OutboxMaxAttempts <= 0 {
		c.OutboxMaxAttempts = defaultOutboxAttempts
	}
	if c.ReconcileInterval <= 0 {
		c.ReconcileInterval = DefaultReconcileInterval
	}
	if c.FullRepairInterval <= 0 {
		c.FullRepairInterval = DefaultFullRepairInterval
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.validateDatabaseFilesystem == nil {
		c.validateDatabaseFilesystem = validateLocalDatabaseFilesystem
	}
	if c.listen == nil {
		listenConfig := &net.ListenConfig{}
		c.listen = listenConfig.Listen
	}
	if c.now == nil {
		c.now = time.Now
	}
	if c.jitter == nil {
		c.jitter = randomJitter
	}
	if c.newLeaseID == nil {
		c.newLeaseID = uuid.NewString
	}
	if c.newTokenID == nil {
		c.newTokenID = uuid.NewString
	}
	if c.generateToken == nil {
		c.generateToken = apikey.GenerateToken
	}
	c.GitHubWebhookSecret = append([]byte(nil), c.GitHubWebhookSecret...)
	c.InitialAdminToken = append([]byte(nil), c.InitialAdminToken...)
	return c
}
