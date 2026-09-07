package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

const webSessionCookieName = "detent_session"

type sessionContextKey struct{}

func webSessionFromContext(ctx context.Context) (auth.Session, bool) {
	session, ok := ctx.Value(sessionContextKey{}).(auth.Session)
	return session, ok
}

func newMagicLinkService(cfg Config, store auth.Store, sender auth.Sender) (*auth.Service, bool, error) {
	authConfig := cfg.GlobalConfig.Auth
	if !authConfig.MagicLinkEnabled() {
		return nil, false, nil
	}
	linkTTL, err := authConfig.LinkTTLDuration()
	if err != nil {
		return nil, false, err
	}
	sessionTTL, err := authConfig.SessionTTLDuration()
	if err != nil {
		return nil, false, err
	}
	if sender == nil {
		sender, err = auth.NewSMTPSender(auth.SMTPConfig{
			Host:     authConfig.SMTP.Host,
			Port:     authConfig.SMTP.NormalizedPort(),
			Username: authConfig.SMTP.Username,
			Password: authConfig.SMTP.Password,
			From:     authConfig.SMTP.From,
		})
		if err != nil {
			return nil, false, fmt.Errorf("configure magic link sender: %w", err)
		}
	}
	publicURL := strings.TrimSpace(authConfig.PublicURL)
	if publicURL == "" {
		publicURL = cfg.dashboardURL()
	}
	service, err := auth.NewService(auth.Config{
		AllowedEmails: authConfig.AllowedEmails,
		LinkTTL:       linkTTL,
		SessionTTL:    sessionTTL,
		PublicURL:     publicURL,
	}, store, sender)
	if err != nil {
		return nil, false, fmt.Errorf("configure magic link auth: %w", err)
	}
	return service, true, nil
}

func (s *Server) sessionGate(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		if s.sessions == nil || sessionRouteExempt(c.Request()) || alternativeAPIAuth(c.Request()) {
			return next(c)
		}
		cookie, err := c.Cookie(webSessionCookieName)
		if err == nil {
			session, authErr := s.sessions.Authenticate(c.Request().Context(), cookie.Value)
			if authErr == nil && session.Identity != nil {
				authErr = auth.ErrInvalidSession
			}
			if authErr == nil {
				ctx := context.WithValue(c.Request().Context(), sessionContextKey{}, session)
				c.SetRequest(c.Request().WithContext(ctx))
				return next(c)
			}
			if !errors.Is(authErr, auth.ErrInvalidSession) {
				s.logger.Error("web session lookup failed", "error", authErr)
				return echo.NewHTTPError(http.StatusServiceUnavailable, "Authentication temporarily unavailable")
			}
			s.clearSessionCookie(c)
		}
		return redirectToLogin(c)
	}
}

func sessionRouteExempt(request *http.Request) bool {
	if request == nil {
		return false
	}
	path := request.URL.Path
	return path == "/health" || path == "/login" || path == "/auth/magic-link" || path == "/auth/oidc/start" || path == "/auth/oidc/callback" || strings.HasPrefix(path, "/static/") || path == openAPIPath || path == "/mcp" || path == "/api/v1/webhooks/github" || strings.HasPrefix(path, "/api/v1/intake/")
}

func alternativeAPIAuth(request *http.Request) bool {
	return request != nil && strings.HasPrefix(request.URL.Path, "/api/") && len(requestAPITokens(request)) > 0
}

func redirectToLogin(c echo.Context) error {
	next := safeReturnPath(c.Request())
	target := "/login"
	if next != "/" {
		target += "?next=" + url.QueryEscape(next)
	}
	if c.Request().Header.Get("HX-Request") == "true" {
		c.Response().Header().Set("HX-Redirect", target)
		return c.NoContent(http.StatusOK)
	}
	if strings.Contains(strings.ToLower(c.Request().Header.Get("Accept")), "text/event-stream") {
		return c.NoContent(http.StatusUnauthorized)
	}
	return c.Redirect(http.StatusSeeOther, target)
}

func safeReturnPath(request *http.Request) string {
	if request == nil || request.Method != http.MethodGet {
		return "/"
	}
	return safeNext(request.URL.RequestURI())
}

func safeNext(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || !strings.HasPrefix(value, "/") || strings.HasPrefix(value, "//") {
		return "/"
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || parsed.IsAbs() || parsed.Host != "" {
		return "/"
	}
	return parsed.RequestURI()
}

func (s *Server) loginPage(c echo.Context) error {
	if s.identityProvider != nil {
		return s.renderAuthPage(c, http.StatusOK, templates.AuthPageOIDCSignIn, safeNext(c.QueryParam("next")))
	}
	return s.renderAuthPage(c, http.StatusOK, templates.AuthPageSignIn, safeNext(c.QueryParam("next")))
}

func (s *Server) requestMagicLink(c echo.Context) error {
	next := safeNext(c.FormValue("next"))
	if err := s.magicLinks.RequestLink(c.Request().Context(), c.FormValue("email"), next); err != nil {
		s.logger.Warn("magic link request failed", "error", err)
	}
	return s.renderAuthPage(c, http.StatusOK, templates.AuthPageCheckInbox, next)
}

func (s *Server) consumeMagicLink(c echo.Context) error {
	c.Response().Header().Set("Referrer-Policy", "no-referrer")
	token, session, err := s.magicLinks.ConsumeLink(c.Request().Context(), c.QueryParam("token"))
	if errors.Is(err, auth.ErrInvalidLink) {
		return s.renderAuthPage(c, http.StatusUnauthorized, templates.AuthPageInvalidLink, safeNext(c.QueryParam("next")))
	}
	if err != nil {
		s.logger.Error("magic link consumption failed", "error", err)
		return s.renderAuthPage(c, http.StatusServiceUnavailable, templates.AuthPageUnavailable, safeNext(c.QueryParam("next")))
	}
	s.setSessionCookie(c, token, session.ExpiresAt)
	return c.Redirect(http.StatusSeeOther, safeNext(c.QueryParam("next")))
}

func (s *Server) renderAuthPage(c echo.Context, status int, state templates.AuthPageState, next string) error {
	c.Response().Header().Set(echo.HeaderCacheControl, "no-store")
	c.Response().Header().Set(echo.HeaderContentType, echo.MIMETextHTMLCharsetUTF8)
	c.Response().WriteHeader(status)
	return templates.AuthPage(templates.AuthPageData{
		Assets: s.assets.templatePaths(),
		Next:   next,
		State:  state,
	}).Render(c.Request().Context(), c.Response().Writer)
}

func (s *Server) setSessionCookie(c echo.Context, token string, expiresAt time.Time) {
	c.SetCookie(&http.Cookie{ // #nosec G124 -- all cookie security attributes are set below; Secure follows the configured HTTP/TLS mode.
		Name:     webSessionCookieName,
		Value:    token,
		Path:     "/",
		Expires:  expiresAt,
		HttpOnly: true,
		Secure:   c.Request().TLS != nil || (s.sessions != nil && s.sessions.SecureCookie()),
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) clearSessionCookie(c echo.Context) {
	c.SetCookie(&http.Cookie{ // #nosec G124 -- the clearing cookie mirrors the protected session cookie attributes.
		Name:     webSessionCookieName,
		Path:     "/",
		Expires:  time.Unix(1, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   c.Request().TLS != nil || (s.sessions != nil && s.sessions.SecureCookie()),
		SameSite: http.SameSiteLaxMode,
	})
}
