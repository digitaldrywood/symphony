package hubserver

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/runnerauth"
)

type apiScope string

const (
	apiScopeWorker      apiScope = "worker"
	apiScopeOperator    apiScope = "operator"
	apiScopeAdmin       apiScope = "admin"
	bootstrapTokenID             = "bootstrap-admin"
	maxBearerTokenBytes          = 4096
)

type apiCredential struct {
	Hosted        *auth.HostedIdentity
	HostedRole    string
	ManageRunners bool
	SessionHash   string
	Hash          string
	ID            string
	Name          string
	Scope         apiScope
	NativeOnly    bool
	Runner        runnerauth.Identity
}

type apiErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type tokenRequest struct {
	Name  string   `json:"name"`
	Scope apiScope `json:"scope"`
}

type tokenResponse struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Scope       apiScope  `json:"scope"`
	Token       string    `json:"token"`
	Fingerprint string    `json:"fingerprint"`
	CreatedAt   time.Time `json:"created_at"`
	RotatedAt   time.Time `json:"rotated_at,omitempty"`
}

func (d *database) ensureInitialAdminToken(ctx context.Context, token []byte) error {
	value := strings.TrimSpace(string(token))
	if value == "" {
		return nil
	}
	now, err := d.currentTime()
	if err != nil {
		return err
	}
	formatted := formatHubTime(now)
	_, err = d.db.ExecContext(ctx, `
INSERT INTO api_tokens (id, name, token_hash, token_fingerprint, scope, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO NOTHING`,
		bootstrapTokenID, "Bootstrap administrator", apikey.HashToken(value), tokenFingerprint(apikey.HashToken(value)), apiScopeAdmin, formatted, formatted,
	)
	if err != nil {
		return fmt.Errorf("store initial hub administrator token: %w", err)
	}
	return nil
}

func (s *Service) requireAPIScope(allowed ...apiScope) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			credential, status, err := s.authenticateAPIRequest(c)
			if err != nil {
				return c.JSON(status, apiErrorResponse{Code: "unauthorized", Message: "Valid scoped API token is required"})
			}
			if s.config.Hosted != nil {
				if credential.Hosted == nil && credential.Runner.RunnerID == "" && !s.hostedArtifactPublisher(c, credential) {
					return s.nativeAPIError(c, nativeNotFound())
				}
				if credential.Hosted != nil && (!strings.HasPrefix(c.Path(), nativeBase) || strings.HasSuffix(c.Path(), "/checks") || strings.Contains(c.Path(), "/imports")) {
					return s.nativeAPIError(c, nativeNotFound())
				}
			}
			if credential.Runner.RunnerID != "" && !runnerOperationAllowed(c, credential.Runner.Operations) {
				return c.JSON(http.StatusForbidden, apiErrorResponse{Code: "insufficient_scope", Message: "Runner does not permit this operation"})
			}
			if strings.HasPrefix(c.Path(), "/api/v1/") {
				if credential.NativeOnly {
					return c.JSON(http.StatusForbidden, apiErrorResponse{Code: "native_protocol_required", Message: "Scoped tokens require the native protocol"})
				}
				if err := s.requireCompatibilityResource(c); err != nil {
					return err
				}
			}
			for _, scope := range allowed {
				if credential.Scope == scope || credential.Scope == apiScopeAdmin {
					c.Set("hub_api_credential", credential)
					return next(c)
				}
			}
			return c.JSON(http.StatusForbidden, apiErrorResponse{Code: "insufficient_scope", Message: "API token scope does not permit this operation"})
		}
	}
}

func (s *Service) authenticateAPIRequest(c echo.Context) (apiCredential, int, error) {
	if s.config.Hosted != nil && c.Request().Header.Get(echo.HeaderAuthorization) == "" {
		return s.hostedCredential(c)
	}
	token, err := apiBearerToken(c)
	if err != nil {
		return apiCredential{}, http.StatusUnauthorized, err
	}
	hash := apikey.HashToken(token)
	var credential apiCredential
	var storedHash, createdAt, operations string
	var revokedAt, expiresAt sql.NullString
	err = s.database.db.QueryRowContext(c.Request().Context(), `
SELECT t.id, t.name, t.scope, t.token_hash, t.revoked_at, t.native_only, t.expires_at, t.created_at,
coalesce(r.id, ''), coalesce(r.machine_id, ''), coalesce(r.organization_id, ''), coalesce(r.operations_json, '[]')
FROM api_tokens t LEFT JOIN runner_identities r ON r.token_id = t.id
WHERE t.token_hash = ?`, hash).Scan(&credential.ID, &credential.Name, &credential.Scope, &storedHash, &revokedAt, &credential.NativeOnly, &expiresAt, &createdAt,
		&credential.Runner.RunnerID, &credential.Runner.MachineID, &credential.Runner.OrganizationID, &operations)
	if errors.Is(err, sql.ErrNoRows) {
		return apiCredential{}, http.StatusUnauthorized, errors.New("token was not found")
	}
	if err != nil {
		return apiCredential{}, http.StatusServiceUnavailable, fmt.Errorf("read hub API token: %w", err)
	}
	if subtle.ConstantTimeCompare([]byte(storedHash), []byte(hash)) != 1 || revokedAt.Valid {
		return apiCredential{}, http.StatusUnauthorized, errors.New("token is inactive")
	}
	now, err := s.database.currentTime()
	if err != nil {
		return apiCredential{}, http.StatusServiceUnavailable, err
	}
	if expiresAt.Valid {
		if !runnerTimeValid(now, createdAt, expiresAt.String) {
			return apiCredential{}, http.StatusUnauthorized, errors.New("token is outside its validity interval")
		}
		credential.Runner.ExpiresAt, err = parseTimeValue(expiresAt.String)
		if err != nil {
			return apiCredential{}, http.StatusServiceUnavailable, err
		}
	}
	if err := json.Unmarshal([]byte(operations), &credential.Runner.Operations); err != nil {
		return apiCredential{}, http.StatusServiceUnavailable, err
	}
	credential.Hash = hash
	if _, err := s.database.db.ExecContext(c.Request().Context(), "UPDATE api_tokens SET last_used_at = ? WHERE id = ?", formatHubTime(now), credential.ID); err != nil {
		return apiCredential{}, http.StatusServiceUnavailable, fmt.Errorf("record hub API token use: %w", err)
	}
	return credential, http.StatusOK, nil
}

func apiBearerToken(c echo.Context) (string, error) {
	if c == nil || c.Request() == nil {
		return "", errors.New("request is required")
	}
	authorizations := c.Request().Header.Values(echo.HeaderAuthorization)
	if len(authorizations) != 1 {
		return "", errors.New("one authorization header is required")
	}
	value := strings.TrimSpace(authorizations[0])
	scheme, token, ok := strings.Cut(value, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", errors.New("bearer authorization is required")
	}
	token = strings.TrimSpace(token)
	if token == "" || len(token) > maxBearerTokenBytes {
		return "", errors.New("bearer token is invalid")
	}
	return token, nil
}

func (s *Service) createAPIToken(c echo.Context) error {
	var request tokenRequest
	if err := decodeAPIJSON(c, &request); err != nil {
		return invalidAPIRequest(c, err)
	}
	request.Name = strings.TrimSpace(request.Name)
	if request.Name == "" || !validAPIScope(request.Scope) {
		return c.JSON(http.StatusUnprocessableEntity, apiErrorResponse{Code: "invalid_token", Message: "Token name and scope are required"})
	}
	token, err := s.config.generateToken()
	if err != nil {
		return s.internalAPIError(c, "token_create_failed", "API token could not be created", err)
	}
	now, err := s.database.currentTime()
	if err != nil {
		return s.internalAPIError(c, "token_create_failed", "API token could not be created", err)
	}
	id := strings.TrimSpace(s.config.newTokenID())
	if id == "" {
		return s.internalAPIError(c, "token_create_failed", "API token could not be created", errors.New("generated token ID is empty"))
	}
	hash := apikey.HashToken(token)
	_, err = s.database.db.ExecContext(c.Request().Context(), `
INSERT INTO api_tokens (id, name, token_hash, token_fingerprint, scope, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id, request.Name, hash, tokenFingerprint(hash), request.Scope, formatHubTime(now), formatHubTime(now),
	)
	if err != nil {
		return c.JSON(http.StatusConflict, apiErrorResponse{Code: "token_conflict", Message: "API token name already exists"})
	}
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusCreated, tokenResponse{ID: id, Name: request.Name, Scope: request.Scope, Token: token, Fingerprint: tokenFingerprint(hash), CreatedAt: now})
}

func (s *Service) rotateAPIToken(c echo.Context) error {
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		return c.JSON(http.StatusNotFound, apiErrorResponse{Code: "token_not_found", Message: "API token was not found"})
	}
	token, err := s.config.generateToken()
	if err != nil {
		return s.internalAPIError(c, "token_rotate_failed", "API token could not be rotated", err)
	}
	now, err := s.database.currentTime()
	if err != nil {
		return s.internalAPIError(c, "token_rotate_failed", "API token could not be rotated", err)
	}
	result, err := s.database.db.ExecContext(c.Request().Context(), `
UPDATE api_tokens
SET token_hash = ?, token_fingerprint = ?, rotated_at = ?, revoked_at = NULL, updated_at = ?
WHERE id = ? AND NOT EXISTS (SELECT 1 FROM runner_identities WHERE token_id = api_tokens.id)`, apikey.HashToken(token), tokenFingerprint(apikey.HashToken(token)), formatHubTime(now), formatHubTime(now), id)
	if err != nil {
		return s.internalAPIError(c, "token_rotate_failed", "API token could not be rotated", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return s.internalAPIError(c, "token_rotate_failed", "API token could not be rotated", err)
	}
	if rows != 1 {
		return c.JSON(http.StatusNotFound, apiErrorResponse{Code: "token_not_found", Message: "API token was not found"})
	}
	var response tokenResponse
	var createdAt string
	err = s.database.db.QueryRowContext(c.Request().Context(), "SELECT id, name, scope, token_fingerprint, created_at FROM api_tokens WHERE id = ?", id).Scan(&response.ID, &response.Name, &response.Scope, &response.Fingerprint, &createdAt)
	if err != nil {
		return s.internalAPIError(c, "token_rotate_failed", "API token could not be rotated", err)
	}
	response.CreatedAt, err = parseTimeValue(createdAt)
	if err != nil {
		return s.internalAPIError(c, "token_rotate_failed", "API token could not be rotated", err)
	}
	response.Token = token
	response.RotatedAt = now
	c.Response().Header().Set("Cache-Control", "no-store")
	return c.JSON(http.StatusOK, response)
}

func (s *Service) revokeAPIToken(c echo.Context) error {
	id := strings.TrimSpace(c.Param("id"))
	now, err := s.database.currentTime()
	if err != nil {
		return s.internalAPIError(c, "token_revoke_failed", "API token could not be revoked", err)
	}
	result, err := s.database.db.ExecContext(c.Request().Context(), "UPDATE api_tokens SET revoked_at = ?, updated_at = ? WHERE id = ? AND revoked_at IS NULL AND NOT EXISTS (SELECT 1 FROM runner_identities WHERE token_id = api_tokens.id)", formatHubTime(now), formatHubTime(now), id)
	if err != nil {
		return s.internalAPIError(c, "token_revoke_failed", "API token could not be revoked", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return s.internalAPIError(c, "token_revoke_failed", "API token could not be revoked", err)
	}
	if rows != 1 {
		return c.JSON(http.StatusNotFound, apiErrorResponse{Code: "token_not_found", Message: "Active API token was not found"})
	}
	return c.NoContent(http.StatusNoContent)
}

func validAPIScope(scope apiScope) bool {
	switch scope {
	case apiScopeWorker, apiScopeOperator, apiScopeAdmin:
		return true
	default:
		return false
	}
}

func tokenFingerprint(hash string) string {
	hash = strings.TrimSpace(hash)
	if len(hash) <= 12 {
		return hash
	}
	return hash[:12]
}
