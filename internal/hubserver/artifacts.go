package hubserver

import (
	"encoding/json"
	"net/http"
	"slices"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func (s *Service) registerArtifactRoutes(e *echo.Echo) {
	read := s.requireNativeScope(apiScopeOperator, apiScopeWorker)
	e.PUT(nativeBase+"/artifact-services/:service", s.bindArtifactService, s.requireNativeScope(apiScopeAdmin))
	e.GET(nativeBase+"/artifact-services", s.artifactServices, read)
	e.POST(nativeBase+"/artifact-services/:service/receipts", s.artifactReceipt, read)
	e.POST(nativeBase+"/artifact-services/:service/authorize", s.authorizeArtifactRead, read)
	e.POST(nativeBase+"/work-items/:item/artifact-authority", s.authorizeArtifactUpload, s.requireNativeScope(apiScopeWorker))
	e.GET(nativeBase+"/work-items/:item/artifacts", s.artifactReferences, read)
	e.POST(nativeBase+"/work-items/:item/artifacts/:artifact/access", s.artifactReadGrant, read)
}

func artifactReadGrantRequest(c echo.Context) bool {
	return c.Request().Method == http.MethodPost && c.Path() == nativeBase+"/work-items/:item/artifacts/:artifact/access"
}

func (s *Service) hostedArtifactPublisher(c echo.Context, credential apiCredential) bool {
	if !credential.NativeOnly || credential.Scope != apiScopeWorker || c.Request().Method != http.MethodPost {
		return false
	}
	if c.Path() != nativeBase+"/artifact-services/:service/receipts" && c.Path() != nativeBase+"/artifact-services/:service/authorize" {
		return false
	}
	var count int
	err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM artifact_services WHERE organization_id=? AND project_id=? AND id=? AND publisher_token_id=?", c.Param("organization"), c.Param("project"), c.Param("service"), credential.ID).Scan(&count)
	return err == nil && count == 1
}

func (s *Service) bindArtifactService(c echo.Context) error {
	var binding artifact.Binding
	if err := decodeAPIJSON(c, &binding); err != nil {
		return invalidAPIRequest(c, err)
	}
	if !artifact.ValidID(binding.ServiceID, "service") || binding.ServiceID != c.Param("service") || !artifact.ValidOrigin(binding.Origin) || !slices.Contains([]string{"customer", "hosted"}, binding.Mode) || binding.Mode == "hosted" && !binding.HostedOptIn || binding.PublisherTokenID == "" {
		return s.nativeAPIError(c, nativeInvalid("Invalid artifact service binding"))
	}
	scope := nativeRequestScope(c)
	var count int
	if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM api_tokens t JOIN token_grants g ON g.token_id=t.id WHERE t.id=? AND t.revoked_at IS NULL AND g.organization_id=? AND g.project_id=?", binding.PublisherTokenID, scope.organization, scope.project).Scan(&count); err != nil {
		return s.nativeAPIError(c, err)
	}
	if count != 1 {
		return s.nativeAPIError(c, nativeNotFound())
	}
	raw, err := json.Marshal(binding)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	_, err = s.database.db.ExecContext(c.Request().Context(), "INSERT INTO artifact_services(organization_id,project_id,id,binding_json,publisher_token_id) VALUES(?,?,?,?,?) ON CONFLICT(organization_id,project_id,id) DO UPDATE SET binding_json=excluded.binding_json,publisher_token_id=excluded.publisher_token_id", scope.organization, scope.project, binding.ServiceID, raw, binding.PublisherTokenID)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, binding)
}

func (s *Service) artifactServices(c echo.Context) error {
	scope := nativeRequestScope(c)
	rows, err := s.database.db.QueryContext(c.Request().Context(), "SELECT binding_json FROM artifact_services WHERE organization_id=? AND project_id=? ORDER BY id LIMIT 16", scope.organization, scope.project)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer rows.Close()
	bindings := []artifact.Binding{}
	for rows.Next() {
		var raw []byte
		var b artifact.Binding
		if err := rows.Scan(&raw); err != nil {
			return s.nativeAPIError(c, err)
		}
		if err := json.Unmarshal(raw, &b); err != nil {
			return s.nativeAPIError(c, err)
		}
		b.PublisherTokenID = ""
		bindings = append(bindings, b)
	}
	if err := rows.Err(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, bindings)
}

func (s *Service) artifactPublisher(c echo.Context) error {
	scope := nativeRequestScope(c)
	var id string
	if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT publisher_token_id FROM artifact_services WHERE organization_id=? AND project_id=? AND id=?", scope.organization, scope.project, c.Param("service")).Scan(&id); err != nil {
		return nativeNotFound()
	}
	if id != scope.credential.ID {
		return nativeNotFound()
	}
	return nil
}

func (s *Service) artifactReceipt(c echo.Context) error {
	if err := s.artifactPublisher(c); err != nil {
		return s.nativeAPIError(c, err)
	}
	var ref artifact.Reference
	if err := decodeAPIJSON(c, &ref); err != nil {
		return invalidAPIRequest(c, err)
	}
	scope := nativeRequestScope(c)
	if ref.SchemaVersion != 1 || ref.Validate() != nil || ref.OrganizationID != string(scope.organization) || ref.ProjectID != string(scope.project) || ref.ServiceID != c.Param("service") || !artifact.ValidID(ref.ArtifactID, "artifact") || !artifact.ValidID(ref.ManifestID, "manifest") || !artifact.ValidHash(ref.SHA256, 64) || ref.Revision < 1 || ref.Revision > artifact.MaxObjects+1 || !slices.Contains([]string{"log", "diff", "screenshot", "video"}, ref.Kind) || !slices.Contains([]string{"partial", "complete", "interrupted"}, ref.State) || ref.Availability != "available" || ref.Bytes < 0 || ref.Bytes > artifact.MaxArtifactBytes || ref.Objects < 0 || ref.Objects > artifact.MaxObjects || ref.ExpiresAt.IsZero() || ref.ObservedAt.IsZero() {
		return s.nativeAPIError(c, nativeInvalid("Invalid artifact receipt"))
	}
	var count int
	if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM native_attempts WHERE organization_id=? AND project_id=? AND work_item_id=? AND id=? AND json_extract(data_json,'$.run_id')=?", scope.organization, scope.project, ref.WorkItemID, ref.AttemptID, ref.RunID).Scan(&count); err != nil {
		return s.nativeAPIError(c, err)
	}
	if count != 1 {
		return s.nativeAPIError(c, nativeNotFound())
	}
	if ref.VersionID != "" {
		if err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM change_versions v JOIN change_requests c ON c.id=v.change_id WHERE v.id=? AND c.organization_id=? AND c.project_id=? AND json_extract(v.record_json,'$.attempt_id')=?", ref.VersionID, scope.organization, scope.project, ref.AttemptID).Scan(&count); err != nil {
			return s.nativeAPIError(c, err)
		}
		if count != 1 {
			return s.nativeAPIError(c, nativeNotFound())
		}
	}
	raw, err := json.Marshal(ref)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	ctx := c.Request().Context()
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer tx.Rollback()
	now := s.config.now()
	before, err := s.database.hostedConsumption(ctx, tx, now)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	inserted, err := tx.ExecContext(ctx, "INSERT INTO artifact_references(organization_id,project_id,work_item_id,service_id,artifact_id,revision,manifest_id,reference_json) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING", scope.organization, scope.project, ref.WorkItemID, ref.ServiceID, ref.ArtifactID, ref.Revision, ref.ManifestID, raw)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	var previous string
	if err := tx.QueryRowContext(ctx, "SELECT reference_json FROM artifact_references WHERE organization_id=? AND project_id=? AND artifact_id=? AND revision=?", scope.organization, scope.project, ref.ArtifactID, ref.Revision).Scan(&previous); err != nil {
		return s.nativeAPIError(c, err)
	}
	if previous != string(raw) {
		return s.nativeAPIError(c, nativeConflict(0))
	}
	added, err := inserted.RowsAffected()
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	if added > 0 {
		if err := s.database.checkHostedGrowth(ctx, tx, before, now, false); err != nil {
			return s.nativeAPIError(c, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Service) artifactReferences(c echo.Context) error {
	scope := nativeRequestScope(c)
	rows, err := s.database.db.QueryContext(c.Request().Context(), "SELECT a.reference_json FROM artifact_references a WHERE a.organization_id=? AND a.project_id=? AND a.work_item_id=? AND a.revision=(SELECT max(b.revision) FROM artifact_references b WHERE b.organization_id=a.organization_id AND b.project_id=a.project_id AND b.artifact_id=a.artifact_id) ORDER BY a.artifact_id LIMIT 64", scope.organization, scope.project, c.Param("item"))
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer rows.Close()
	refs := []artifact.Reference{}
	for rows.Next() {
		var raw []byte
		var ref artifact.Reference
		if err := rows.Scan(&raw); err != nil {
			return s.nativeAPIError(c, err)
		}
		if err := json.Unmarshal(raw, &ref); err != nil {
			return s.nativeAPIError(c, err)
		}
		if !s.config.now().Before(ref.ExpiresAt) {
			ref.Availability = "expired"
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, refs)
}

func (s *Service) authorizeArtifactUpload(c echo.Context) error {
	var r artifact.Reservation
	if err := decodeAPIJSON(c, &r); err != nil {
		return invalidAPIRequest(c, err)
	}
	scope := nativeRequestScope(c)
	if r.Validate() != nil || r.OrganizationID != string(scope.organization) || r.ProjectID != string(scope.project) || r.WorkItemID != c.Param("item") || r.LeaseID == "" || r.FencingToken <= 0 || scope.credential.Scope != apiScopeWorker {
		return s.nativeAPIError(c, nativeNotFound())
	}
	tx, err := s.database.db.BeginTx(c.Request().Context(), nil)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer tx.Rollback()
	var bindingRaw []byte
	if err := tx.QueryRowContext(c.Request().Context(), "SELECT binding_json FROM artifact_services WHERE organization_id=? AND project_id=? AND id=?", scope.organization, scope.project, r.ServiceID).Scan(&bindingRaw); err != nil {
		return s.nativeAPIError(c, nativeNotFound())
	}
	var binding artifact.Binding
	if err := json.Unmarshal(bindingRaw, &binding); err != nil {
		return s.nativeAPIError(c, err)
	}
	if r.Mode != binding.Mode || r.HostedOptIn != binding.HostedOptIn {
		return s.nativeAPIError(c, nativeNotFound())
	}
	if err := requireNativeMutationLease(c.Request().Context(), tx, scope, r.WorkItemID, tracker.Mutation{LeaseID: tracker.LeaseID(r.LeaseID), FencingToken: tracker.FencingToken(r.FencingToken)}, s.config.now()); err != nil {
		return s.nativeAPIError(c, err)
	}
	var count int
	if err := tx.QueryRowContext(c.Request().Context(), "SELECT count(*) FROM native_attempts WHERE id=? AND lease_id=? AND organization_id=? AND project_id=? AND work_item_id=? AND status='running' AND json_extract(data_json,'$.run_id')=?", r.AttemptID, r.LeaseID, scope.organization, scope.project, r.WorkItemID, r.RunID).Scan(&count); err != nil {
		return s.nativeAPIError(c, err)
	}
	if count != 1 {
		return s.nativeAPIError(c, nativeNotFound())
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Service) artifactReadGrant(c echo.Context) error {
	var input struct {
		Revision int64 `json:"revision"`
	}
	if err := decodeAPIJSON(c, &input); err != nil {
		return invalidAPIRequest(c, err)
	}
	scope := nativeRequestScope(c)
	ctx := c.Request().Context()
	var raw, bindingRaw []byte
	if err := s.database.db.QueryRowContext(ctx, "SELECT a.reference_json,s.binding_json FROM artifact_references a JOIN artifact_services s ON s.organization_id=a.organization_id AND s.project_id=a.project_id AND s.id=a.service_id WHERE a.organization_id=? AND a.project_id=? AND a.work_item_id=? AND a.artifact_id=? AND a.revision=?", scope.organization, scope.project, c.Param("item"), c.Param("artifact"), input.Revision).Scan(&raw, &bindingRaw); err != nil {
		return s.nativeAPIError(c, err)
	}
	var ref artifact.Reference
	var binding artifact.Binding
	if err := json.Unmarshal(raw, &ref); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := json.Unmarshal(bindingRaw, &binding); err != nil {
		return s.nativeAPIError(c, err)
	}
	now := s.config.now().UTC()
	expires := minTime(now.Add(time.Minute), ref.ExpiresAt)
	if scope.credential.Hosted != nil {
		expires = minTime(expires, scope.credential.Hosted.ExpiresAt)
	}
	if !now.Before(expires) {
		return c.JSON(http.StatusGone, apiErrorResponse{Code: "expired", Message: "Artifact retention has expired"})
	}
	token, err := apikey.GenerateToken()
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return s.nativeAPIError(c, err)
	}
	defer tx.Rollback()
	var count int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM token_grants g JOIN api_tokens t ON t.id=g.token_id WHERE g.token_id=? AND g.organization_id=? AND g.project_id=? AND t.revoked_at IS NULL", scope.credential.ID, scope.organization, scope.project).Scan(&count); err != nil {
		return s.nativeAPIError(c, err)
	}
	if count != 1 {
		return s.nativeAPIError(c, nativeNotFound())
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM artifact_grants WHERE expires_at<=?", now.Unix()); err != nil {
		return s.nativeAPIError(c, err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO artifact_grants(token_hash,principal_id,organization_id,project_id,service_id,artifact_id,revision,expires_at,hosted_session_hash) VALUES(?,?,?,?,?,?,?,?,?)", apikey.HashToken(token), scope.credential.ID, scope.organization, scope.project, ref.ServiceID, ref.ArtifactID, ref.Revision, expires.Unix(), scope.credential.SessionHash); err != nil {
		return s.nativeAPIError(c, err)
	}
	if err := tx.Commit(); err != nil {
		return s.nativeAPIError(c, err)
	}
	return c.JSON(http.StatusOK, artifact.Grant{Token: token, Origin: binding.Origin, ArtifactID: ref.ArtifactID, Revision: ref.Revision, SHA256: ref.SHA256, ExpiresAt: expires})
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (s *Service) authorizeArtifactRead(c echo.Context) error {
	if err := s.artifactPublisher(c); err != nil {
		return s.nativeAPIError(c, err)
	}
	var r artifact.ReadAuthorization
	if err := decodeAPIJSON(c, &r); err != nil {
		return invalidAPIRequest(c, err)
	}
	if r.Token == "" || len(r.Token) > 4096 || !artifact.ValidID(r.ArtifactID, "artifact") || r.Revision < 1 {
		return s.nativeAPIError(c, nativeNotFound())
	}
	scope := nativeRequestScope(c)
	var created, principal, sessionHash string
	var expires *string
	var deadline int64
	err := s.database.db.QueryRowContext(c.Request().Context(), "SELECT t.created_at,t.expires_at,a.expires_at,a.principal_id,a.hosted_session_hash FROM artifact_grants a JOIN api_tokens t ON t.id=a.principal_id JOIN token_grants g ON g.token_id=t.id AND g.organization_id=a.organization_id AND g.project_id=a.project_id WHERE a.token_hash=? AND a.organization_id=? AND a.project_id=? AND a.service_id=? AND a.artifact_id=? AND a.revision=? AND t.revoked_at IS NULL", apikey.HashToken(r.Token), scope.organization, scope.project, c.Param("service"), r.ArtifactID, r.Revision).Scan(&created, &expires, &deadline, &principal, &sessionHash)
	if err != nil || s.config.now().Unix() >= deadline || expires != nil && !runnerTimeValid(s.config.now(), created, *expires) {
		return s.nativeAPIError(c, nativeNotFound())
	}
	if s.config.Hosted != nil {
		if err := s.authorizeHostedArtifactRead(c, principal, sessionHash); err != nil {
			return s.nativeAPIError(c, nativeNotFound())
		}
	}
	return c.NoContent(http.StatusNoContent)
}

func (s *Service) authorizeHostedArtifactRead(c echo.Context, principal, sessionHash string) error {
	ctx := c.Request().Context()
	session, err := s.WebSession(ctx, sessionHash, s.config.now())
	if err != nil {
		return err
	}
	credential, _, err := s.hostedSessionCredential(ctx, session, sessionHash)
	if err != nil || credential.ID != principal {
		return auth.ErrHostedIdentity
	}
	scope := nativeRequestScope(c)
	scope.credential = credential
	if err := s.requireHostedProject(ctx, s.database.db, scope, false); err != nil {
		return err
	}
	return s.hostedAudit(ctx, session.Identity, "action", c.Request().Method+" "+c.Path(), string(scope.project), 0)
}
