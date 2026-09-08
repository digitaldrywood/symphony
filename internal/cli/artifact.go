package cli

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/digitaldrywood/detent/internal/artifact"
)

type configuredArtifactAllowances struct{ limits artifact.Limits }

func (a configuredArtifactAllowances) Limits(context.Context, string) (artifact.Limits, error) {
	return a.limits, nil
}

func newArtifactCommand(lookup func(string) string) *cobra.Command {
	var configPath, projectID, address string
	cmd := &cobra.Command{Use: "artifacts", Short: "Operate an independent artifact service", Example: "detent artifacts --artifact-config service.json serve", Args: NoArgs}
	cmd.PersistentFlags().StringVar(&configPath, "artifact-config", "", "private artifact service JSON configuration")
	load := func() (cfg artifact.Config, resultErr error) {
		file, err := os.Open(configPath)
		if err != nil {
			return cfg, err
		}
		defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
		if err := artifact.Decode(file, &cfg, artifact.MaxManifestBytes); err != nil {
			return cfg, err
		}
		return cfg, cfg.Validate()
	}
	serve := &cobra.Command{Use: "serve", Short: "Serve artifacts without execution runners", Example: "detent artifacts --artifact-config service.json serve --listen 127.0.0.1:7788", Args: NoArgs, RunE: func(cmd *cobra.Command, _ []string) (resultErr error) {
		cfg, err := load()
		if err != nil {
			return err
		}
		if !artifact.ValidOrigin(cfg.HubOrigin) || projectID != "" && !artifact.ValidID(projectID, "prj") || cfg.PublishTokenEnv == "" || lookup(cfg.PublishTokenEnv) == "" {
			return artifact.ErrInvalid
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil || port == "4000" || host != "127.0.0.1" && host != "::1" {
			return errors.New("artifact listener must use loopback behind a TLS reverse proxy and cannot use port 4000")
		}
		storage, err := artifact.NewStorage(cmd.Context(), cfg.Storage)
		if err != nil {
			return err
		}
		if err := artifact.VerifyStorage(cmd.Context(), storage); err != nil {
			return err
		}
		hub := &artifact.RemoteHub{Origin: cfg.HubOrigin, ServiceID: cfg.ServiceID, OrganizationID: cfg.OrganizationID, ProjectID: projectID, PublisherToken: func() string { return strings.TrimSpace(lookup(cfg.PublishTokenEnv)) }}
		var allowances artifact.Allowances = configuredArtifactAllowances{cfg.Policy.Limits}
		if cfg.Mode == "hosted" {
			allowances = hub
		}
		service, err := artifact.NewService(cmd.Context(), cfg, storage, allowances)
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, service.Close()) }()
		hub.Usage = service.Usage
		httpServer, err := artifact.NewHTTPServer(service, hub)
		if err != nil {
			return err
		}
		listener, err := (&net.ListenConfig{}).Listen(cmd.Context(), "tcp", address)
		if err != nil {
			return err
		}
		return httpServer.Serve(cmd.Context(), listener, hub)
	}}
	serve.Flags().StringVar(&projectID, "project", "", "optional restriction to one native project ID")
	serve.Flags().StringVar(&address, "listen", "127.0.0.1:7788", "loopback listener behind TLS")
	verify := &cobra.Command{Use: "verify-storage", Short: "Probe the configured bucket for required artifact capabilities", Example: "detent artifacts --artifact-config service.json verify-storage", Args: NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := load()
		if err != nil {
			return err
		}
		storage, err := artifact.NewStorage(cmd.Context(), cfg.Storage)
		if err != nil {
			return err
		}
		return artifact.VerifyStorage(cmd.Context(), storage)
	}}
	var destination string
	operate := func(cmd *cobra.Command, backup bool) (resultErr error) {
		cfg, err := load()
		if err != nil {
			return err
		}
		storage, err := artifact.NewStorage(cmd.Context(), cfg.Storage)
		if err != nil {
			return err
		}
		service, err := artifact.NewService(cmd.Context(), cfg, storage, configuredArtifactAllowances{cfg.Policy.Limits})
		if err != nil {
			return err
		}
		defer func() { resultErr = errors.Join(resultErr, service.Close()) }()
		if backup {
			return service.Backup(cmd.Context(), destination)
		}
		usage, err := service.Usage(cmd.Context())
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(usage)
	}
	backup := &cobra.Command{Use: "backup", Short: "Back up the catalog while its service is stopped", Example: "detent artifacts --artifact-config service.json backup --output backup.db", Args: NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return operate(cmd, true) }}
	backup.Flags().StringVar(&destination, "output", "", "new private backup file")
	usage := &cobra.Command{Use: "usage", Short: "Read retained and reserved bytes while the service is stopped", Example: "detent artifacts --artifact-config service.json usage", Args: NoArgs, RunE: func(cmd *cobra.Command, _ []string) error { return operate(cmd, false) }}
	cmd.AddCommand(serve, verify, backup, usage)
	return cmd
}
