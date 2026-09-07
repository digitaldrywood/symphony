package web

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/web/templates"
)

const (
	oidcTransactionCookieName = "detent_oidc_transaction"
	oidcTransactionTTL        = 10 * time.Minute
	oidcDiscoveryTimeout      = 10 * time.Second
)

type oidcTransaction struct {
	State     string `json:"state"`
	Nonce     string `json:"nonce"`
	Verifier  string `json:"verifier"`
	Next      string `json:"next"`
	ExpiresAt int64  `json:"expires_at"`
}

func newOIDCService(ctx context.Context, cfg Config, store auth.SessionStore, provider auth.IdentityProvider) (auth.IdentityProvider, *auth.Service, *auth.Allowlist, bool, error) {
	authConfig := cfg.GlobalConfig.Auth
	if !authConfig.OIDCEnabled() {
		return nil, nil, nil, false, nil
	}
	sessionTTL, err := authConfig.SessionTTLDuration()
	if err != nil {
		return nil, nil, nil, false, err
	}
	publicURL := strings.TrimSpace(authConfig.PublicURL)
	if publicURL == "" {
		publicURL = cfg.dashboardURL()
	}
	sessions, err := auth.NewSessionService(auth.SessionConfig{SessionTTL: sessionTTL, PublicURL: publicURL}, store)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("configure oidc sessions: %w", err)
	}
	allowlist, err := auth.NewAllowlist(authConfig.AllowedEmails, authConfig.AllowedDomains)
	if err != nil {
		return nil, nil, nil, false, fmt.Errorf("configure oidc allowlist: %w", err)
	}
	redirectURL, err := oidcCallbackURL(publicURL)
	if err != nil {
		return nil, nil, nil, false, err
	}
	if provider == nil {
		discoveryCtx, cancel := context.WithTimeout(ctx, oidcDiscoveryTimeout)
		defer cancel()
		provider, err = auth.NewIdentityProvider(discoveryCtx, auth.IdentityProviderOIDC, auth.OIDCConfig{
			IssuerURL:    authConfig.OIDC.IssuerURL,
			ClientID:     authConfig.OIDC.ClientID,
			ClientSecret: authConfig.OIDC.ClientSecret,
			RedirectURL:  redirectURL,
			Scopes:       authConfig.OIDC.Scopes,
		})
		if err != nil {
			return nil, nil, nil, false, fmt.Errorf("configure oidc provider: %w", err)
		}
	}
	return provider, sessions, allowlist, true, nil
}

func oidcCallbackURL(publicURL string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(publicURL))
	if err != nil || parsed == nil || parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("oidc public URL must be absolute")
	}
	if parsed.Scheme != "https" && (parsed.Scheme != "http" || !serverAddressLoopback(parsed.Host)) {
		return "", errors.New("oidc public URL must use https; loopback http is allowed for testing")
	}
	parsed.Path = "/auth/oidc/callback"
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func (s *Server) startOIDC(c echo.Context) error {
	transaction, err := newOIDCTransaction(safeNext(c.QueryParam("next")))
	if err != nil {
		s.logger.Error("create oidc transaction failed")
		return s.renderAuthPage(c, http.StatusServiceUnavailable, templates.AuthPageUnavailable, transaction.Next)
	}
	sealed, err := s.sealOIDCTransaction(transaction)
	if err != nil {
		s.logger.Error("seal oidc transaction failed")
		return s.renderAuthPage(c, http.StatusServiceUnavailable, templates.AuthPageUnavailable, transaction.Next)
	}
	s.setOIDCTransactionCookie(c, sealed, time.Unix(transaction.ExpiresAt, 0))
	return c.Redirect(http.StatusSeeOther, s.identityProvider.AuthorizationURL(transaction.State, transaction.Nonce, transaction.Verifier))
}

func (s *Server) completeOIDC(c echo.Context) error {
	c.Response().Header().Set("Referrer-Policy", "no-referrer")
	transaction, err := s.oidcTransaction(c)
	s.clearOIDCTransactionCookie(c)
	if err != nil || !hmac.Equal([]byte(transaction.State), []byte(c.QueryParam("state"))) || time.Now().After(time.Unix(transaction.ExpiresAt, 0)) {
		return s.renderAuthPage(c, http.StatusUnauthorized, templates.AuthPageInvalidIdentity, safeNext(transaction.Next))
	}
	if c.QueryParam("error") != "" {
		state := templates.AuthPageInvalidIdentity
		status := http.StatusUnauthorized
		if c.QueryParam("error") == "access_denied" {
			state = templates.AuthPageDenied
			status = http.StatusForbidden
		}
		return s.renderAuthPage(c, status, state, transaction.Next)
	}
	identity, err := s.identityProvider.Exchange(c.Request().Context(), c.QueryParam("code"), transaction.Verifier, transaction.Nonce)
	if err != nil {
		if errors.Is(err, auth.ErrOIDCUnverifiedEmail) || errors.Is(err, auth.ErrOIDCMissingEmail) {
			return s.renderAuthPage(c, http.StatusForbidden, templates.AuthPageDenied, transaction.Next)
		}
		s.logger.Warn("oidc callback rejected", "reason", oidcFailureReason(err))
		return s.renderAuthPage(c, http.StatusUnauthorized, templates.AuthPageInvalidIdentity, transaction.Next)
	}
	if identity.Hosted != nil || !s.identityAllowlist.Allows(identity.Email, identity.EmailVerified) {
		return s.renderAuthPage(c, http.StatusForbidden, templates.AuthPageDenied, transaction.Next)
	}
	token, session, err := s.sessions.CreateSession(c.Request().Context(), identity.Email)
	if err != nil {
		s.logger.Error("create oidc web session failed")
		return s.renderAuthPage(c, http.StatusServiceUnavailable, templates.AuthPageUnavailable, transaction.Next)
	}
	s.setSessionCookie(c, token, session.ExpiresAt)
	return c.Redirect(http.StatusSeeOther, transaction.Next)
}

func newOIDCTransaction(next string) (oidcTransaction, error) {
	state, err := oidcRandomValue(32)
	if err != nil {
		return oidcTransaction{Next: next}, err
	}
	nonce, err := oidcRandomValue(32)
	if err != nil {
		return oidcTransaction{Next: next}, err
	}
	verifier, err := oidcRandomValue(32)
	if err != nil {
		return oidcTransaction{Next: next}, err
	}
	return oidcTransaction{
		State:     state,
		Nonce:     nonce,
		Verifier:  verifier,
		Next:      next,
		ExpiresAt: time.Now().Add(oidcTransactionTTL).Unix(),
	}, nil
}

func oidcRandomValue(size int) (string, error) {
	raw := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		return "", fmt.Errorf("generate oidc random value: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Server) sealOIDCTransaction(transaction oidcTransaction) (string, error) {
	plaintext, err := json.Marshal(transaction)
	if err != nil {
		return "", fmt.Errorf("marshal oidc transaction: %w", err)
	}
	aead, err := s.oidcTransactionAEAD()
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate oidc transaction nonce: %w", err)
	}
	sealed := aead.Seal(nonce, nonce, plaintext, []byte(oidcTransactionCookieName))
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

func (s *Server) oidcTransaction(c echo.Context) (oidcTransaction, error) {
	cookie, err := c.Cookie(oidcTransactionCookieName)
	if err != nil {
		return oidcTransaction{}, errors.New("oidc transaction cookie is missing")
	}
	sealed, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil {
		return oidcTransaction{}, errors.New("oidc transaction cookie is invalid")
	}
	aead, err := s.oidcTransactionAEAD()
	if err != nil {
		return oidcTransaction{}, err
	}
	if len(sealed) < aead.NonceSize() {
		return oidcTransaction{}, errors.New("oidc transaction cookie is invalid")
	}
	nonce := sealed[:aead.NonceSize()]
	plaintext, err := aead.Open(nil, nonce, sealed[aead.NonceSize():], []byte(oidcTransactionCookieName))
	if err != nil {
		return oidcTransaction{}, errors.New("oidc transaction cookie is invalid")
	}
	var transaction oidcTransaction
	if err := json.Unmarshal(plaintext, &transaction); err != nil {
		return oidcTransaction{}, errors.New("oidc transaction cookie is invalid")
	}
	return transaction, nil
}

func (s *Server) oidcTransactionAEAD() (cipher.AEAD, error) {
	block, err := aes.NewCipher(s.dashboardAuthSecret[:])
	if err != nil {
		return nil, fmt.Errorf("create oidc transaction cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create oidc transaction AEAD: %w", err)
	}
	return aead, nil
}

func (s *Server) setOIDCTransactionCookie(c echo.Context, value string, expiresAt time.Time) {
	c.SetCookie(&http.Cookie{ // #nosec G124 -- all cookie security attributes are set below; Secure follows the configured HTTP/TLS mode.
		Name:     oidcTransactionCookieName,
		Value:    value,
		Path:     "/auth/oidc",
		Expires:  expiresAt,
		MaxAge:   int(oidcTransactionTTL / time.Second),
		HttpOnly: true,
		Secure:   c.Request().TLS != nil || (s.sessions != nil && s.sessions.SecureCookie()),
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Server) clearOIDCTransactionCookie(c echo.Context) {
	c.SetCookie(&http.Cookie{ // #nosec G124 -- the clearing cookie mirrors the protected transaction cookie attributes.
		Name:     oidcTransactionCookieName,
		Path:     "/auth/oidc",
		Expires:  time.Unix(1, 0),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   c.Request().TLS != nil || (s.sessions != nil && s.sessions.SecureCookie()),
		SameSite: http.SameSiteLaxMode,
	})
}

func oidcFailureReason(err error) string {
	switch {
	case errors.Is(err, auth.ErrOIDCExchange):
		return "exchange"
	case errors.Is(err, auth.ErrOIDCInvalidNonce):
		return "nonce"
	case errors.Is(err, auth.ErrOIDCInvalidToken):
		return "token"
	default:
		return "identity"
	}
}
