package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

const tokenBytes = 32

var (
	ErrEmailNotAllowed = errors.New("email is not allowed")
	ErrInvalidLink     = errors.New("magic link is invalid or expired")
	ErrInvalidSession  = errors.New("web session is invalid or expired")
	ErrMissingStore    = errors.New("auth store is required")
	ErrMissingSender   = errors.New("magic link sender is required")
)

type Config struct {
	AllowedEmails []string
	LinkTTL       time.Duration
	SessionTTL    time.Duration
	PublicURL     string
}

type SessionConfig struct {
	SessionTTL time.Duration
	PublicURL  string
}

type MagicLink struct {
	TokenHash string
	Email     string
	ExpiresAt time.Time
	CreatedAt time.Time
}

type MagicLinkConsumption struct {
	TokenHash        string
	SessionHash      string
	SessionExpiresAt time.Time
	Now              time.Time
}

type SessionRecord struct {
	TokenHash string
	Email     string
	ExpiresAt time.Time
	CreatedAt time.Time
	Identity  *HostedIdentity
}

type Session struct {
	Email     string
	ExpiresAt time.Time
	Identity  *HostedIdentity
}

type Message struct {
	To        string
	URL       string
	ExpiresAt time.Time
}

type SessionStore interface {
	CreateWebSession(context.Context, SessionRecord) error
	WebSession(context.Context, string, time.Time) (Session, error)
}

type MagicLinkStore interface {
	CreateMagicLink(context.Context, MagicLink) error
	ConsumeMagicLink(context.Context, MagicLinkConsumption) (Session, error)
}

type Store interface {
	SessionStore
	MagicLinkStore
}

type Sender interface {
	SendMagicLink(context.Context, Message) error
}

type Option func(*Service)

type Service struct {
	sessions   SessionStore
	magicLinks MagicLinkStore
	sender     Sender
	allowed    map[string]struct{}
	linkTTL    time.Duration
	sessionTTL time.Duration
	publicURL  *url.URL
	now        func() time.Time
	random     io.Reader
}

func WithClock(now func() time.Time) Option {
	return func(service *Service) {
		service.now = now
	}
}

func WithRandom(reader io.Reader) Option {
	return func(service *Service) {
		service.random = reader
	}
}

func NewService(cfg Config, store Store, sender Sender, opts ...Option) (*Service, error) {
	if cfg.LinkTTL <= 0 {
		return nil, errors.New("magic link ttl must be positive")
	}
	service, err := NewSessionService(SessionConfig{SessionTTL: cfg.SessionTTL, PublicURL: cfg.PublicURL}, store, opts...)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(cfg.AllowedEmails))
	for _, email := range cfg.AllowedEmails {
		email = normalizeEmail(email)
		if email != "" {
			allowed[email] = struct{}{}
		}
	}
	if len(allowed) == 0 {
		return nil, errors.New("at least one allowed email is required")
	}

	service.magicLinks = store
	service.sender = sender
	service.allowed = allowed
	service.linkTTL = cfg.LinkTTL
	return service, nil
}

func NewSessionService(cfg SessionConfig, store SessionStore, opts ...Option) (*Service, error) {
	if store == nil {
		return nil, ErrMissingStore
	}
	if cfg.SessionTTL <= 0 {
		return nil, errors.New("session ttl must be positive")
	}
	publicURL, err := parsePublicURL(cfg.PublicURL)
	if err != nil {
		return nil, err
	}
	service := &Service{
		sessions:   store,
		sessionTTL: cfg.SessionTTL,
		publicURL:  publicURL,
		now:        time.Now,
		random:     rand.Reader,
	}
	for _, opt := range opts {
		opt(service)
	}
	if service.now == nil {
		service.now = time.Now
	}
	if service.random == nil {
		service.random = rand.Reader
	}
	return service, nil
}

func (s *Service) RequestLink(ctx context.Context, email string, next string) error {
	email = normalizeEmail(email)
	if !s.emailAllowed(email) {
		return nil
	}
	if s.sender == nil {
		return ErrMissingSender
	}
	link, expiresAt, err := s.createLink(ctx, email, next)
	if err != nil {
		return err
	}
	if err := s.sender.SendMagicLink(ctx, Message{To: email, URL: link, ExpiresAt: expiresAt}); err != nil {
		return fmt.Errorf("send magic link: %w", err)
	}
	return nil
}

func (s *Service) CreateLink(ctx context.Context, email string, next string) (string, time.Time, error) {
	email = normalizeEmail(email)
	if !s.emailAllowed(email) {
		return "", time.Time{}, ErrEmailNotAllowed
	}
	return s.createLink(ctx, email, next)
}

func (s *Service) ConsumeLink(ctx context.Context, token string) (string, Session, error) {
	if strings.TrimSpace(token) == "" {
		return "", Session{}, ErrInvalidLink
	}
	sessionToken, err := s.newToken()
	if err != nil {
		return "", Session{}, err
	}
	now := s.now().UTC()
	session, err := s.magicLinks.ConsumeMagicLink(ctx, MagicLinkConsumption{
		TokenHash:        tokenHash(token),
		SessionHash:      tokenHash(sessionToken),
		SessionExpiresAt: now.Add(s.sessionTTL),
		Now:              now,
	})
	if err != nil {
		return "", Session{}, err
	}
	return sessionToken, session, nil
}

func (s *Service) CreateSession(ctx context.Context, email string) (string, Session, error) {
	return s.createSession(ctx, email, nil)
}

func (s *Service) CreateIdentitySession(ctx context.Context, identity Identity) (string, Session, error) {
	if !identity.EmailVerified || identity.Hosted == nil || identity.Subject != identity.Hosted.Subject || !identity.Hosted.ExpiresAt.After(s.now()) {
		return "", Session{}, ErrInvalidSession
	}
	return s.createSession(ctx, identity.Email, identity.Hosted)
}

func (s *Service) createSession(ctx context.Context, email string, identity *HostedIdentity) (string, Session, error) {
	token, err := s.newToken()
	if err != nil {
		return "", Session{}, err
	}
	now := s.now().UTC()
	session := Session{Email: normalizeEmail(email), ExpiresAt: now.Add(s.sessionTTL)}
	session.Identity = identity
	if identity != nil && identity.ExpiresAt.Before(session.ExpiresAt) {
		session.ExpiresAt = identity.ExpiresAt
	}
	if session.Email == "" {
		return "", Session{}, errors.New("session email is required")
	}
	if err := s.sessions.CreateWebSession(ctx, SessionRecord{
		TokenHash: tokenHash(token),
		Email:     session.Email,
		ExpiresAt: session.ExpiresAt,
		CreatedAt: now,
		Identity:  identity,
	}); err != nil {
		return "", Session{}, fmt.Errorf("create web session: %w", err)
	}
	return token, session, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (Session, error) {
	if strings.TrimSpace(token) == "" {
		return Session{}, ErrInvalidSession
	}
	session, err := s.sessions.WebSession(ctx, tokenHash(token), s.now().UTC())
	if err != nil {
		return Session{}, err
	}
	return session, nil
}

func (s *Service) SecureCookie() bool {
	return s != nil && s.publicURL != nil && strings.EqualFold(s.publicURL.Scheme, "https")
}

func (s *Service) createLink(ctx context.Context, email string, next string) (string, time.Time, error) {
	token, err := s.newToken()
	if err != nil {
		return "", time.Time{}, err
	}
	now := s.now().UTC()
	expiresAt := now.Add(s.linkTTL)
	if err := s.magicLinks.CreateMagicLink(ctx, MagicLink{
		TokenHash: tokenHash(token),
		Email:     email,
		ExpiresAt: expiresAt,
		CreatedAt: now,
	}); err != nil {
		return "", time.Time{}, fmt.Errorf("create magic link: %w", err)
	}
	link := *s.publicURL
	link.Path = "/auth/magic-link"
	query := link.Query()
	query.Set("token", token)
	if next = safeNext(next); next != "/" {
		query.Set("next", next)
	}
	link.RawQuery = query.Encode()
	return link.String(), expiresAt, nil
}

func (s *Service) newToken() (string, error) {
	raw := make([]byte, tokenBytes)
	if _, err := io.ReadFull(s.random, raw); err != nil {
		return "", fmt.Errorf("generate auth token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) emailAllowed(email string) bool {
	if s == nil {
		return false
	}
	_, ok := s.allowed[normalizeEmail(email)]
	return ok
}

func parsePublicURL(value string) (*url.URL, error) {
	value = strings.TrimSpace(value)
	parsed, err := url.Parse(value)
	if err != nil || parsed == nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return nil, errors.New("auth public url must be an absolute http or https URL")
	}
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed, nil
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

func normalizeEmail(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func tokenHash(token string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(token)))
	return hex.EncodeToString(sum[:])
}
