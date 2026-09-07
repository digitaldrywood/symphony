package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	connectorgithub "github.com/digitaldrywood/detent/internal/connector/github"
	"github.com/digitaldrywood/detent/internal/hubgithub"
	"github.com/digitaldrywood/detent/internal/hubserver"
)

type hubRunFunc func(context.Context, hubserver.Config) error

func newHubCommand(opts options) *cobra.Command {
	run := func(ctx context.Context, cfg hubserver.Config) error {
		if cfg.GitHubDisabled {
			return hubserver.Run(ctx, cfg)
		}
		token, err := opts.ghAuthToken(ctx)
		if err != nil {
			return fmt.Errorf("resolve github credentials for hub outbox: %w", err)
		}
		client, err := connectorgithub.NewClient(connectorgithub.ClientConfig{
			TokenSource: connectorgithub.StaticTokenSource(token),
			Logger:      cfg.Logger,
		})
		if err != nil {
			return fmt.Errorf("configure github hub outbox client: %w", err)
		}
		transport := hubgithub.NewTransport(client)
		cfg.OutboxBackend = hubgithub.NewWriter(transport)
		cfg.ReconcileBackend = hubgithub.NewReconciler(transport)
		cfg.ImportBackend = hubgithub.NewImporter(transport)
		cfg.GitHubRequestCounts = transport.Counts
		return hubserver.Run(ctx, cfg)
	}
	return newHubCommandWithRun(opts.version, opts.lookupEnv, run)
}

func newHubCommandWithRun(version string, lookupEnv func(string) string, run hubRunFunc) *cobra.Command {
	if lookupEnv == nil {
		lookupEnv = os.Getenv
	}
	cmd := &cobra.Command{
		Use:     "hub",
		Short:   "Run the shared Detent Hub service",
		Example: "detent hub serve --database /var/lib/detent/hub.db",
		Args:    NoArgs,
	}
	cmd.AddCommand(newHubServeCommand(version, lookupEnv, run))
	cmd.AddCommand(newHubRunnerCommand(version, lookupEnv))
	cmd.AddCommand(newHubPolicyCommand(lookupEnv))
	cmd.AddCommand(newHubIssueCommand(lookupEnv))
	cmd.AddCommand(newHubRecoveryCommands(lookupEnv)...)
	return cmd
}

func newHubServeCommand(version string, lookupEnv func(string) string, run hubRunFunc) *cobra.Command {
	var githubDisabled bool
	var databasePath string
	var listenAddress string
	var tlsCertificateFile string
	var tlsKeyFile string
	var trustedProxy bool
	var adminTokenEnv string
	var busyTimeout time.Duration
	var shutdownTimeout time.Duration
	var githubWebhookSecretEnv string
	var webhookPayloadRetention time.Duration
	var webhookMaintenanceInterval time.Duration
	var reconcileInterval time.Duration
	var fullRepairInterval time.Duration

	cmd := &cobra.Command{
		Use:          "serve",
		Short:        "Serve the Detent Hub API",
		Example:      "detent hub serve --database /var/lib/detent/hub.db --listen 127.0.0.1:7777",
		Args:         NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := OutputForCommand(cmd); err != nil {
				return err
			}
			if strings.TrimSpace(databasePath) == "" {
				return NewValidationError("hub database path is required", "Run detent hub serve --database /path/to/hub.db.", nil)
			}
			if strings.TrimSpace(listenAddress) == "" {
				return NewValidationError("hub listen address is required", "Use --listen 127.0.0.1:7777 or another explicit address.", nil)
			}
			adminTokenEnv = strings.TrimSpace(adminTokenEnv)
			if !validEnvName(adminTokenEnv) {
				return NewValidationError("Hub administrator token environment variable name is invalid", "Use an environment variable name such as DETENT_HUB_ADMIN_TOKEN.", nil)
			}
			adminToken := strings.TrimSpace(lookupEnv(adminTokenEnv))
			if adminToken == "" {
				return NewValidationError("Hub administrator token is required", "Set "+adminTokenEnv+" to a high-entropy API token.", nil)
			}
			githubWebhookSecretEnv = strings.TrimSpace(githubWebhookSecretEnv)
			if githubWebhookSecretEnv != "" && !validEnvName(githubWebhookSecretEnv) {
				return NewValidationError("Hub GitHub webhook secret environment variable name is invalid", "Use an environment variable name such as DETENT_HUB_GITHUB_WEBHOOK_SECRET.", nil)
			}
			return run(cmd.Context(), hubserver.Config{
				GitHubDisabled:             githubDisabled,
				DatabasePath:               databasePath,
				ListenAddress:              listenAddress,
				TLSCertFile:                strings.TrimSpace(tlsCertificateFile),
				TLSKeyFile:                 strings.TrimSpace(tlsKeyFile),
				TrustedProxy:               trustedProxy,
				InitialAdminToken:          []byte(adminToken),
				BusyTimeout:                busyTimeout,
				ShutdownTimeout:            shutdownTimeout,
				GitHubWebhookSecret:        []byte(strings.TrimSpace(lookupEnv(githubWebhookSecretEnv))),
				WebhookPayloadRetention:    webhookPayloadRetention,
				WebhookMaintenanceInterval: webhookMaintenanceInterval,
				ReconcileInterval:          reconcileInterval,
				FullRepairInterval:         fullRepairInterval,
				Logger:                     slog.Default(),
				Version:                    version,
			})
		},
	}
	cmd.Flags().StringVar(&databasePath, "database", "", "local filesystem path to the Hub SQLite database")
	cmd.Flags().BoolVar(&githubDisabled, "github-disabled", false, "serve native collaboration without GitHub credentials or transport")
	cmd.Flags().StringVar(&listenAddress, "listen", hubserver.DefaultListenAddress, "Hub listen address")
	cmd.Flags().StringVar(&tlsCertificateFile, "tls-cert", "", "TLS certificate file")
	cmd.Flags().StringVar(&tlsKeyFile, "tls-key", "", "TLS private key file")
	cmd.Flags().BoolVar(&trustedProxy, "trusted-proxy", false, "declare that a trusted reverse proxy terminates TLS")
	cmd.Flags().StringVar(&adminTokenEnv, "admin-token-env", "DETENT_HUB_ADMIN_TOKEN", "environment variable containing the initial Hub administrator token")
	cmd.Flags().DurationVar(&busyTimeout, "busy-timeout", 5*time.Second, "SQLite busy timeout")
	cmd.Flags().DurationVar(&shutdownTimeout, "shutdown-timeout", 5*time.Second, "graceful HTTP shutdown timeout")
	cmd.Flags().StringVar(&githubWebhookSecretEnv, "github-webhook-secret-env", "DETENT_HUB_GITHUB_WEBHOOK_SECRET", "environment variable containing the GitHub webhook secret")
	cmd.Flags().DurationVar(&webhookPayloadRetention, "webhook-payload-retention", hubserver.DefaultWebhookPayloadRetention, "retention period for raw GitHub webhook payloads")
	cmd.Flags().DurationVar(&webhookMaintenanceInterval, "webhook-maintenance-interval", hubserver.DefaultWebhookMaintenanceInterval, "GitHub webhook retry and retention interval")
	cmd.Flags().DurationVar(&reconcileInterval, "reconcile-interval", hubserver.DefaultReconcileInterval, "incremental GitHub mirror reconciliation interval")
	cmd.Flags().DurationVar(&fullRepairInterval, "full-repair-interval", hubserver.DefaultFullRepairInterval, "full GitHub mirror repair interval")
	return cmd
}
