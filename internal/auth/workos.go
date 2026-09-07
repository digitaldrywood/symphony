package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/mail"
	"net/netip"
	"net/url"
	"strings"
	"time"

	coreoidc "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

const (
	IdentityProviderWorkOS = "workos"
	workosResponseLimit    = 1 << 20
	workosPageLimit        = 100
)

type WorkOSConfig struct {
	APIURL      string
	IssuerURL   string
	ClientID    string
	APIKey      string
	RedirectURL string
	HTTPClient  *http.Client
}

type workosProvider struct {
	apiURL      string
	issuerURL   string
	clientID    string
	apiKey      string
	redirectURL string
	client      *http.Client
	verifier    *coreoidc.IDTokenVerifier
}

type workosTransport struct {
	base http.RoundTripper
}

func (t workosTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	response, err := t.base.RoundTrip(request)
	if err == nil {
		response.Body = http.MaxBytesReader(nil, response.Body, workosResponseLimit)
	}
	return response, err
}

type workosActor struct {
	Email  string `json:"email"`
	Reason string `json:"reason"`
}

type workosUser struct {
	ID            string `json:"id"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
}

type workosSession struct {
	ID             string       `json:"id"`
	UserID         string       `json:"user_id"`
	OrganizationID string       `json:"organization_id"`
	Status         string       `json:"status"`
	CreatedAt      time.Time    `json:"created_at"`
	ExpiresAt      time.Time    `json:"expires_at"`
	EndedAt        *time.Time   `json:"ended_at"`
	Impersonator   *workosActor `json:"impersonator"`
}

type workosPage[T any] struct {
	Data     []T `json:"data"`
	Metadata struct {
		After string `json:"after"`
	} `json:"list_metadata"`
}

func NewHostedProvider(provider string, cfg WorkOSConfig) (HostedProvider, error) {
	if strings.ToLower(strings.TrimSpace(provider)) != IdentityProviderWorkOS {
		return nil, errors.New("unsupported hosted identity provider")
	}
	if cfg.APIURL == "" {
		cfg.APIURL = "https://api.workos.com"
	}
	if cfg.IssuerURL == "" {
		cfg.IssuerURL = cfg.APIURL
	}
	if !validWorkOSURL(cfg.APIURL, true) || !validWorkOSURL(cfg.IssuerURL, false) || !validWorkOSURL(cfg.RedirectURL, false) || !validWorkOSID(cfg.ClientID) || strings.TrimSpace(cfg.APIKey) == "" || strings.ContainsAny(cfg.APIKey, "\r\n") {
		return nil, errors.New("invalid hosted identity configuration")
	}
	client := http.Client{Timeout: 10 * time.Second}
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
		if client.Timeout <= 0 {
			client.Timeout = 10 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client.Transport = workosTransport{base: transport}
	p := &workosProvider{
		apiURL: strings.TrimSuffix(cfg.APIURL, "/"), issuerURL: strings.TrimSuffix(cfg.IssuerURL, "/"),
		clientID: cfg.ClientID, apiKey: cfg.APIKey, redirectURL: cfg.RedirectURL, client: &client,
	}
	keyContext := context.WithValue(context.Background(), oauth2.HTTPClient, &client)
	keySet := coreoidc.NewRemoteKeySet(keyContext, p.apiURL+"/sso/jwks/"+url.PathEscape(p.clientID))
	p.verifier = coreoidc.NewVerifier(p.issuerURL, keySet, &coreoidc.Config{
		SkipClientIDCheck: true, SkipIssuerCheck: true, SupportedSigningAlgs: []string{coreoidc.RS256},
	})
	return p, nil
}

func validWorkOSURL(value string, root bool) bool {
	u, err := url.Parse(value)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (root && u.Path != "" && u.Path != "/") {
		return false
	}
	if u.Scheme == "https" {
		return true
	}
	if u.Scheme != "http" {
		return false
	}
	if strings.EqualFold(u.Hostname(), "localhost") {
		return true
	}
	address, err := netip.ParseAddr(u.Hostname())
	return err == nil && address.IsLoopback()
}

func validWorkOSID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character < 'a' || character > 'z' {
			if character < 'A' || character > 'Z' {
				if (character < '0' || character > '9') && character != '_' && character != '-' {
					return false
				}
			}
		}
	}
	return true
}

func validWorkOSEmail(email string) bool {
	parsed, err := mail.ParseAddress(email)
	return err == nil && parsed != nil && parsed.Address == email && !strings.ContainsAny(email, "\r\n")
}

func (p *workosProvider) AuthorizationURL(state string, _ string, verifier string) string {
	query := url.Values{
		"provider": {"authkit"}, "response_type": {"code"}, "client_id": {p.clientID},
		"redirect_uri": {p.redirectURL}, "state": {state},
		"code_challenge": {oauth2.S256ChallengeFromVerifier(verifier)}, "code_challenge_method": {"S256"},
	}
	return p.apiURL + "/user_management/authorize?" + query.Encode()
}

func (p *workosProvider) Exchange(ctx context.Context, code string, verifier string, _ string) (Identity, error) {
	if strings.TrimSpace(code) == "" {
		return Identity{}, ErrHostedIdentity
	}
	request := struct {
		GrantType    string `json:"grant_type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
		Code         string `json:"code"`
		CodeVerifier string `json:"code_verifier,omitempty"`
	}{"authorization_code", p.clientID, p.apiKey, code, verifier}
	var response struct {
		User           workosUser   `json:"user"`
		AccessToken    string       `json:"access_token"`
		OrganizationID string       `json:"organization_id"`
		Impersonator   *workosActor `json:"impersonator"`
	}
	if err := p.request(ctx, http.MethodPost, "/user_management/authenticate", request, &response); err != nil {
		return Identity{}, err
	}
	identity, err := p.verifyToken(ctx, response.AccessToken, response.Impersonator)
	if err != nil || identity.Subject != response.User.ID || identity.OrganizationID != response.OrganizationID || (strings.TrimSpace(verifier) == "" && identity.SupportActor == "") {
		return Identity{}, ErrHostedIdentity
	}
	email := normalizeEmail(response.User.Email)
	if !response.User.EmailVerified || !validWorkOSEmail(email) {
		return Identity{}, ErrHostedIdentity
	}
	session, err := p.session(ctx, identity)
	if err != nil {
		return Identity{}, err
	}
	identity.CreatedAt = session.CreatedAt
	identity.ExpiresAt = session.ExpiresAt
	if identity.SupportActor != "" && identity.ExpiresAt.After(identity.CreatedAt.Add(time.Hour)) {
		identity.ExpiresAt = identity.CreatedAt.Add(time.Hour)
	}
	if !identity.ExpiresAt.After(time.Now()) {
		return Identity{}, ErrHostedIdentity
	}
	return Identity{Subject: identity.Subject, Email: email, EmailVerified: true, Hosted: &identity}, nil
}

func (p *workosProvider) verifyToken(ctx context.Context, raw string, actor *workosActor) (HostedIdentity, error) {
	token, err := p.verifier.Verify(ctx, raw)
	if err != nil || (token.Issuer != p.issuerURL && token.Issuer != p.issuerURL+"/") || !validWorkOSID(token.Subject) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	now := time.Now()
	if token.IssuedAt.IsZero() || token.IssuedAt.After(now.Add(5*time.Minute)) || !token.Expiry.After(token.IssuedAt) || !token.Expiry.After(now) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	var claims struct {
		ClientID       string          `json:"client_id"`
		SessionID      string          `json:"sid"`
		OrganizationID string          `json:"org_id"`
		Audience       json.RawMessage `json:"aud"`
		Actor          *struct {
			Email   string `json:"email"`
			Subject string `json:"sub"`
		} `json:"act"`
	}
	if err := token.Claims(&claims); err != nil || claims.ClientID != p.clientID || !validWorkOSID(claims.SessionID) || (claims.OrganizationID != "" && !validWorkOSID(claims.OrganizationID)) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	if len(claims.Audience) > 0 {
		matched := false
		for _, audience := range token.Audience {
			matched = matched || audience == p.clientID
		}
		if !matched {
			return HostedIdentity{}, ErrHostedIdentity
		}
	}
	identity := HostedIdentity{Subject: token.Subject, OrganizationID: claims.OrganizationID, SessionID: claims.SessionID}
	if (claims.Actor == nil) != (actor == nil) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	if actor != nil {
		email := normalizeEmail(actor.Email)
		if !validWorkOSEmail(email) || !ValidSupportReason(actor.Reason) || identity.OrganizationID == "" {
			return HostedIdentity{}, ErrHostedIdentity
		}
		signedEmail, signedSubject := normalizeEmail(claims.Actor.Email), normalizeEmail(claims.Actor.Subject)
		if (signedEmail == "" && signedSubject == "") || (signedEmail != "" && signedEmail != email) || (signedSubject != "" && signedSubject != email) {
			return HostedIdentity{}, ErrHostedIdentity
		}
		identity.SupportActor, identity.SupportReason = email, actor.Reason
	}
	return identity, nil
}

func (p *workosProvider) CurrentSession(ctx context.Context, identity HostedIdentity) (HostedIdentity, error) {
	if identity.CreatedAt.IsZero() || !identity.ExpiresAt.After(time.Now()) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	session, err := p.session(ctx, identity)
	if err != nil || !session.CreatedAt.Equal(identity.CreatedAt) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	if session.ExpiresAt.Before(identity.ExpiresAt) {
		identity.ExpiresAt = session.ExpiresAt
	}
	if identity.SupportActor != "" && identity.ExpiresAt.After(identity.CreatedAt.Add(time.Hour)) {
		identity.ExpiresAt = identity.CreatedAt.Add(time.Hour)
	}
	if !identity.ExpiresAt.After(time.Now()) {
		return HostedIdentity{}, ErrHostedIdentity
	}
	return identity, nil
}

func (p *workosProvider) session(ctx context.Context, identity HostedIdentity) (workosSession, error) {
	if !validWorkOSID(identity.Subject) || !validWorkOSID(identity.SessionID) {
		return workosSession{}, ErrHostedIdentity
	}
	query := url.Values{"limit": {"100"}}
	seen := make(map[string]bool)
	for range workosPageLimit {
		var page workosPage[workosSession]
		if err := p.request(ctx, http.MethodGet, "/user_management/users/"+identity.Subject+"/sessions?"+query.Encode(), nil, &page); err != nil {
			return workosSession{}, err
		}
		for _, session := range page.Data {
			if session.ID == identity.SessionID {
				if !validWorkOSSession(session, identity) {
					return workosSession{}, ErrHostedIdentity
				}
				return session, nil
			}
		}
		if page.Metadata.After == "" {
			break
		}
		if !validWorkOSID(page.Metadata.After) || seen[page.Metadata.After] {
			return workosSession{}, ErrHostedIdentity
		}
		seen[page.Metadata.After] = true
		query.Set("after", page.Metadata.After)
	}
	return workosSession{}, ErrHostedIdentity
}

func validWorkOSSession(session workosSession, identity HostedIdentity) bool {
	now := time.Now()
	if session.Status != "active" || session.UserID != identity.Subject || session.OrganizationID != identity.OrganizationID || session.EndedAt != nil || session.CreatedAt.IsZero() || session.CreatedAt.After(now.Add(time.Minute)) || !session.ExpiresAt.After(now) || !session.ExpiresAt.After(session.CreatedAt) {
		return false
	}
	if session.Impersonator == nil {
		return identity.SupportActor == "" && identity.SupportReason == ""
	}
	return identity.SupportActor != "" && normalizeEmail(session.Impersonator.Email) == identity.SupportActor && session.Impersonator.Reason == identity.SupportReason && ValidSupportReason(identity.SupportReason)
}

func (p *workosProvider) Memberships(ctx context.Context, userID string, organizationID string) ([]Membership, error) {
	if (userID == "" && organizationID == "") || (userID != "" && !validWorkOSID(userID)) || (organizationID != "" && !validWorkOSID(organizationID)) {
		return nil, ErrHostedIdentity
	}
	query := url.Values{"limit": {"100"}, "statuses": {"active"}}
	if userID != "" {
		query.Set("user_id", userID)
	}
	if organizationID != "" {
		query.Set("organization_id", organizationID)
	}
	var memberships []Membership
	seen := make(map[string]bool)
	for range workosPageLimit {
		var page workosPage[Membership]
		if err := p.request(ctx, http.MethodGet, "/user_management/organization_memberships?"+query.Encode(), nil, &page); err != nil {
			return nil, err
		}
		for _, membership := range page.Data {
			if !validWorkOSMembership(membership) || (userID != "" && membership.UserID != userID) || (organizationID != "" && membership.OrganizationID != organizationID) {
				return nil, ErrHostedIdentity
			}
			memberships = append(memberships, membership)
		}
		if page.Metadata.After == "" {
			return memberships, nil
		}
		if !validWorkOSID(page.Metadata.After) || seen[page.Metadata.After] {
			return nil, ErrHostedIdentity
		}
		seen[page.Metadata.After] = true
		query.Set("after", page.Metadata.After)
	}
	return nil, ErrHostedIdentity
}

func validWorkOSMembership(membership Membership) bool {
	return validWorkOSID(membership.ID) && validWorkOSID(membership.UserID) && validWorkOSID(membership.OrganizationID) && membership.Status == "active" && ValidOrganizationRole(membership.Role.Slug)
}

func (p *workosProvider) Organization(ctx context.Context, id string) (Organization, error) {
	if !validWorkOSID(id) {
		return Organization{}, ErrHostedIdentity
	}
	var organization Organization
	if err := p.request(ctx, http.MethodGet, "/organizations/"+id, nil, &organization); err != nil {
		return Organization{}, err
	}
	if organization.ID != id {
		return Organization{}, ErrHostedIdentity
	}
	return organization, nil
}

func (p *workosProvider) CreateOrganization(ctx context.Context, externalID string, name string) (Organization, error) {
	if !validWorkOSID(externalID) || len(externalID) > 64 || strings.TrimSpace(name) == "" || len(name) > 256 {
		return Organization{}, ErrHostedIdentity
	}
	request := struct {
		ExternalID string `json:"external_id"`
		Name       string `json:"name"`
	}{externalID, name}
	var organization Organization
	if err := p.request(ctx, http.MethodPost, "/organizations", request, &organization); err != nil {
		if lookupErr := p.request(ctx, http.MethodGet, "/organizations/external_id/"+externalID, nil, &organization); lookupErr != nil {
			return Organization{}, lookupErr
		}
	}
	if !validWorkOSID(organization.ID) || organization.ExternalID != externalID {
		return Organization{}, ErrHostedIdentity
	}
	return organization, nil
}

func (p *workosProvider) CreateMembership(ctx context.Context, userID string, organizationID string, role string) (Membership, error) {
	if !validWorkOSID(userID) || !validWorkOSID(organizationID) || !ValidOrganizationRole(role) {
		return Membership{}, ErrHostedIdentity
	}
	request := struct {
		UserID         string `json:"user_id"`
		OrganizationID string `json:"organization_id"`
		Role           string `json:"role_slug"`
	}{userID, organizationID, role}
	var membership Membership
	if err := p.request(ctx, http.MethodPost, "/user_management/organization_memberships", request, &membership); err != nil {
		memberships, lookupErr := p.Memberships(ctx, userID, organizationID)
		if lookupErr != nil || len(memberships) != 1 || memberships[0].Role.Slug != role {
			return Membership{}, ErrHostedIdentity
		}
		membership = memberships[0]
	}
	if !validWorkOSMembership(membership) || membership.UserID != userID || membership.OrganizationID != organizationID || membership.Role.Slug != role {
		return Membership{}, ErrHostedIdentity
	}
	return membership, nil
}

func (p *workosProvider) SetMembershipRole(ctx context.Context, id string, role string) error {
	if !validWorkOSID(id) || !ValidOrganizationRole(role) {
		return ErrHostedIdentity
	}
	request := struct {
		Role string `json:"role_slug"`
	}{role}
	var membership Membership
	if err := p.request(ctx, http.MethodPut, "/user_management/organization_memberships/"+id, request, &membership); err != nil {
		return err
	}
	if !validWorkOSMembership(membership) || membership.ID != id || membership.Role.Slug != role {
		return ErrHostedIdentity
	}
	return nil
}

func (p *workosProvider) RevokeMembership(ctx context.Context, id string) error {
	if !validWorkOSID(id) {
		return ErrHostedIdentity
	}
	return p.request(ctx, http.MethodPut, "/user_management/organization_memberships/"+id+"/deactivate", nil, nil)
}

func (p *workosProvider) Invite(ctx context.Context, organizationID string, email string, role string, inviterID string) (Invitation, error) {
	email = normalizeEmail(email)
	if !validWorkOSID(organizationID) || !validWorkOSID(inviterID) || !ValidOrganizationRole(role) || !validWorkOSEmail(email) {
		return Invitation{}, ErrHostedIdentity
	}
	request := struct {
		OrganizationID string `json:"organization_id"`
		Email          string `json:"email"`
		Role           string `json:"role_slug"`
		InviterID      string `json:"inviter_user_id"`
	}{organizationID, email, role, inviterID}
	var invitation Invitation
	if err := p.request(ctx, http.MethodPost, "/user_management/invitations", request, &invitation); err != nil {
		return Invitation{}, err
	}
	if !validWorkOSInvitation(invitation) || invitation.State != "pending" || normalizeEmail(invitation.Email) != email || invitation.OrganizationID != organizationID {
		return Invitation{}, ErrHostedIdentity
	}
	return invitation, nil
}

func (p *workosProvider) Invitation(ctx context.Context, token string) (Invitation, error) {
	if !validWorkOSID(token) {
		return Invitation{}, ErrHostedIdentity
	}
	var invitation Invitation
	if err := p.request(ctx, http.MethodGet, "/user_management/invitations/by_token/"+token, nil, &invitation); err != nil {
		return Invitation{}, err
	}
	if !validWorkOSInvitation(invitation) {
		return Invitation{}, ErrHostedIdentity
	}
	return invitation, nil
}

func validWorkOSInvitation(invitation Invitation) bool {
	if !validWorkOSID(invitation.ID) || !validWorkOSID(invitation.OrganizationID) || !validWorkOSEmail(normalizeEmail(invitation.Email)) || !invitation.ExpiresAt.After(time.Now()) {
		return false
	}
	return invitation.State == "pending" && invitation.AcceptedUserID == "" || invitation.State == "accepted" && validWorkOSID(invitation.AcceptedUserID)
}

func (p *workosProvider) AcceptInvitation(ctx context.Context, token string, userID string) error {
	if !validWorkOSID(userID) {
		return ErrHostedIdentity
	}
	invitation, err := p.Invitation(ctx, token)
	if err != nil {
		return err
	}
	if invitation.State != "pending" {
		return ErrHostedIdentity
	}
	var user workosUser
	if err := p.request(ctx, http.MethodGet, "/user_management/users/"+userID, nil, &user); err != nil {
		return err
	}
	if user.ID != userID || !user.EmailVerified || normalizeEmail(user.Email) != normalizeEmail(invitation.Email) {
		return ErrHostedIdentity
	}
	var accepted Invitation
	if err := p.request(ctx, http.MethodPost, "/user_management/invitations/"+invitation.ID+"/accept", nil, &accepted); err != nil {
		return err
	}
	if accepted.ID != invitation.ID || accepted.OrganizationID != invitation.OrganizationID || normalizeEmail(accepted.Email) != normalizeEmail(invitation.Email) || accepted.State != "accepted" || accepted.AcceptedUserID != userID {
		return ErrHostedIdentity
	}
	return nil
}

func (p *workosProvider) RevokeSession(ctx context.Context, id string) error {
	if !validWorkOSID(id) {
		return ErrHostedIdentity
	}
	request := struct {
		SessionID string `json:"session_id"`
	}{id}
	return p.request(ctx, http.MethodPost, "/user_management/sessions/revoke", request, nil)
}

func (p *workosProvider) request(ctx context.Context, method string, path string, payload any, result any) error {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return ErrHostedIdentity
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, method, p.apiURL+path, body)
	if err != nil {
		return ErrHostedIdentity
	}
	request.Header.Set("Authorization", "Bearer "+p.apiKey)
	request.Header.Set("Accept", "application/json")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := p.client.Do(request)
	if err != nil {
		return ErrHostedIdentity
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return ErrHostedIdentity
	}
	if result == nil {
		return nil
	}
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(result); err != nil {
		return ErrHostedIdentity
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrHostedIdentity
	}
	return nil
}
