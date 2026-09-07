package auth

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
	"strings"
	"time"

	coreoidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const IdentityProviderOIDC = "oidc"

var (
	ErrOIDCExchange        = errors.New("oidc authorization code exchange failed")
	ErrOIDCInvalidToken    = errors.New("oidc ID token is invalid")
	ErrOIDCInvalidNonce    = errors.New("oidc nonce is invalid")
	ErrOIDCMissingEmail    = errors.New("oidc ID token does not contain an email")
	ErrOIDCUnverifiedEmail = errors.New("oidc email is not verified")
)

type OIDCConfig struct {
	IssuerURL    string
	ClientID     string
	ClientSecret string
	RedirectURL  string
	Scopes       []string
}

type Identity struct {
	Subject       string
	Email         string
	EmailVerified bool
	Hosted        *HostedIdentity
}

type IdentityProvider interface {
	AuthorizationURL(state string, nonce string, verifier string) string
	Exchange(context.Context, string, string, string) (Identity, error)
}

type oidcProvider struct {
	oauth2Config oauth2.Config
	verifier     *coreoidc.IDTokenVerifier
	issuerURL    string
	clientID     string
}

func NewIdentityProvider(ctx context.Context, provider string, cfg OIDCConfig) (IdentityProvider, error) {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case IdentityProviderOIDC:
		return newOIDCProvider(ctx, cfg)
	default:
		return nil, fmt.Errorf("unsupported identity provider %q", provider)
	}
}

func newOIDCProvider(ctx context.Context, cfg OIDCConfig) (*oidcProvider, error) {
	issuerURL := strings.TrimSpace(cfg.IssuerURL)
	clientID := strings.TrimSpace(cfg.ClientID)
	redirectURL := strings.TrimSpace(cfg.RedirectURL)
	if issuerURL == "" || clientID == "" || cfg.ClientSecret == "" || redirectURL == "" {
		return nil, errors.New("oidc issuer URL, client ID, client secret, and redirect URL are required")
	}
	provider, err := coreoidc.NewProvider(ctx, issuerURL)
	if err != nil {
		return nil, fmt.Errorf("discover oidc provider: %w", err)
	}
	return &oidcProvider{
		oauth2Config: oauth2.Config{
			ClientID:     clientID,
			ClientSecret: cfg.ClientSecret,
			Endpoint:     provider.Endpoint(),
			RedirectURL:  redirectURL,
			Scopes:       oidcScopes(cfg.Scopes),
		},
		verifier:  provider.Verifier(&coreoidc.Config{ClientID: clientID}),
		issuerURL: issuerURL,
		clientID:  clientID,
	}, nil
}

func (p *oidcProvider) AuthorizationURL(state string, nonce string, verifier string) string {
	return p.oauth2Config.AuthCodeURL(
		state,
		coreoidc.Nonce(nonce),
		oauth2.S256ChallengeOption(verifier),
	)
}

func (p *oidcProvider) Exchange(ctx context.Context, code string, verifier string, nonce string) (Identity, error) {
	if strings.TrimSpace(code) == "" || strings.TrimSpace(verifier) == "" || strings.TrimSpace(nonce) == "" {
		return Identity{}, ErrOIDCExchange
	}
	token, err := p.oauth2Config.Exchange(ctx, code, oauth2.VerifierOption(verifier))
	if err != nil {
		return Identity{}, ErrOIDCExchange
	}
	rawIDToken, ok := token.Extra("id_token").(string)
	if !ok || strings.TrimSpace(rawIDToken) == "" {
		return Identity{}, ErrOIDCInvalidToken
	}
	idToken, err := p.verifier.Verify(ctx, rawIDToken)
	if err != nil {
		return Identity{}, ErrOIDCInvalidToken
	}
	var claims struct {
		Email           string `json:"email"`
		EmailVerified   bool   `json:"email_verified"`
		Nonce           string `json:"nonce"`
		AuthorizedParty string `json:"azp"`
	}
	if err := idToken.Claims(&claims); err != nil || strings.TrimSpace(idToken.Subject) == "" || idToken.Issuer != p.issuerURL {
		return Identity{}, ErrOIDCInvalidToken
	}
	now := time.Now()
	if idToken.IssuedAt.IsZero() || idToken.IssuedAt.After(now.Add(5*time.Minute)) || !idToken.Expiry.After(idToken.IssuedAt) {
		return Identity{}, ErrOIDCInvalidToken
	}
	if (len(idToken.Audience) > 1 && claims.AuthorizedParty == "") || (claims.AuthorizedParty != "" && claims.AuthorizedParty != p.clientID) {
		return Identity{}, ErrOIDCInvalidToken
	}
	if !hmac.Equal([]byte(claims.Nonce), []byte(nonce)) {
		return Identity{}, ErrOIDCInvalidNonce
	}
	email := normalizeEmail(claims.Email)
	if email == "" {
		return Identity{}, ErrOIDCMissingEmail
	}
	if !claims.EmailVerified {
		return Identity{}, ErrOIDCUnverifiedEmail
	}
	return Identity{
		Subject:       idToken.Subject,
		Email:         email,
		EmailVerified: true,
	}, nil
}

func oidcScopes(configured []string) []string {
	ordered := []string{coreoidc.ScopeOpenID, "email"}
	seen := map[string]struct{}{coreoidc.ScopeOpenID: {}, "email": {}}
	for _, scope := range configured {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, ok := seen[scope]; ok {
			continue
		}
		seen[scope] = struct{}{}
		ordered = append(ordered, scope)
	}
	return ordered
}
