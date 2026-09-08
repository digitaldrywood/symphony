package hubserver

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
)

func validateCredentialMaintenance(cfg Config) error {
	if !cfg.CredentialMaintenance {
		return nil
	}
	if cfg.Hosted == nil || cfg.Hosted.WorkOSOrganizationID == "" || !listenerAddressLoopback(cfg.ListenAddress) || cfg.TrustedProxy {
		return errors.New("credential maintenance requires hosted configuration with an explicit provider organization and private loopback without a trusted proxy")
	}
	return nil
}

func (s *Service) registerCredentialMaintenanceRoutes(e *echo.Echo) {
	admin := s.requireCredentialMaintenance
	e.POST("/api/v1/tokens", s.createAPIToken, admin)
	e.POST("/api/v1/tokens/:id/rotate", s.rotateAPIToken, admin)
	e.DELETE("/api/v1/tokens/:id", s.revokeAPIToken, admin)
	e.POST("/api/v2/tokens/:id/grants", s.grantNativeToken, admin)
}

func (s *Service) requireCredentialMaintenance(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		c.Response().Header().Set("Cache-Control", "no-store")
		if _, err := apiBearerToken(c); err != nil || c.Request().Header.Get("Origin") != "" {
			return c.NoContent(http.StatusUnauthorized)
		}
		credential, status, err := s.authenticateAPIRequest(c)
		if err != nil {
			return c.NoContent(status)
		}
		if credential.Scope != apiScopeAdmin || credential.NativeOnly || credential.Hosted != nil || credential.Runner.RunnerID != "" {
			return c.NoContent(http.StatusForbidden)
		}
		var members int
		if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM hosted_members WHERE principal_id = ?", credential.ID).Scan(&members); err != nil || members != 0 {
			return c.NoContent(http.StatusForbidden)
		}
		identity := &auth.HostedIdentity{Subject: credential.ID, CreatedAt: s.config.now()}
		if err := s.hostedAudit(c.Request().Context(), identity, "credential_maintenance", c.Request().Method+" "+c.Request().URL.Path, "", 0); err != nil {
			return s.nativeAPIError(c, err)
		}
		c.Set("hub_api_credential", credential)
		return next(c)
	}
}
