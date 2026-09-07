package hubserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/artifact"
)

func seedHostedArtifact(t *testing.T, f hostedSecurityFixture) (artifact.Reference, string, string) {
	t.Helper()
	item := f.seedIssue(t, 1)
	now := time.Now().UTC()
	publisher := "artifact-publisher-token"
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO api_tokens(id,name,token_hash,token_fingerprint,scope,native_only,created_at,updated_at) VALUES ('artifact-publisher','publisher',?,'fingerprint','worker',1,?,?)", apikey.HashToken(publisher), formatHubTime(now), formatHubTime(now)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES ('artifact-publisher','org_security',?)", f.project); err != nil {
		t.Fatal(err)
	}
	binding := artifact.Binding{ServiceID: artifact.NewID("service"), Origin: "https://artifacts.example.test", Mode: "customer", PublisherTokenID: "artifact-publisher"}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO artifact_services(organization_id,project_id,id,binding_json,publisher_token_id) VALUES ('org_security',?,?,?,'artifact-publisher')", f.project, binding.ServiceID, raw); err != nil {
		t.Fatal(err)
	}
	ref := artifact.Reference{SchemaVersion: 1, Scope: artifact.Scope{OrganizationID: "org_security", ProjectID: string(f.project), WorkItemID: string(item)}, ServiceID: binding.ServiceID, ArtifactID: artifact.NewID("artifact"), ManifestID: artifact.NewID("manifest"), Revision: 1, SHA256: artifact.Digest([]byte("manifest")), Kind: "log", State: "partial", Availability: "available", Bytes: 100, Objects: 1, ExpiresAt: now.Add(time.Hour), ObservedAt: now}
	raw, err = json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO artifact_references(organization_id,project_id,work_item_id,service_id,artifact_id,revision,manifest_id,reference_json) VALUES ('org_security',?,?,?,?,1,?,?)", f.project, item, binding.ServiceID, ref.ArtifactID, ref.ManifestID, raw); err != nil {
		t.Fatal(err)
	}
	return ref, publisher, f.base + "/artifact-services/" + binding.ServiceID
}

func TestHostedArtifactReadPermissions(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, role, projectGrant, email, support string
		want                                     int
	}{
		{name: "viewer", role: "viewer", projectGrant: "read", want: http.StatusOK},
		{name: "member read", role: "member", projectGrant: "read", want: http.StatusOK},
		{name: "member write", role: "member", projectGrant: "write", want: http.StatusOK},
		{name: "owner without project", role: "owner", want: http.StatusNotFound},
		{name: "admin without project", role: "admin", want: http.StatusNotFound},
		{name: "staff", role: "member", projectGrant: "write", email: "staff@example.test", want: http.StatusForbidden},
		{name: "support viewer", role: "viewer", projectGrant: "read", support: "support@example.test", want: http.StatusOK},
		{name: "support without project", role: "member", support: "support@example.test", want: http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			ref, publisher, servicePath := seedHostedArtifact(t, f)
			email := test.email
			if email == "" {
				email = "customer@example.test"
			}
			user := f.user(t, "customer", test.role, email, test.projectGrant, test.support)
			path := f.base + "/work-items/" + ref.WorkItemID + "/artifacts/" + ref.ArtifactID + "/access"
			response := f.request(t, user, http.MethodPost, path, map[string]int64{"revision": 1})
			requireNativeStatus(t, response, test.want)
			if test.want != http.StatusOK {
				return
			}
			var grant artifact.Grant
			decodeHubResponse(t, response, &grant)
			request := artifact.ReadAuthorization{Token: grant.Token, ArtifactID: ref.ArtifactID, Revision: 1}
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, request), http.StatusNoContent)
			if response.Header().Get("Cache-Control") != "no-store" || grant.ExpiresAt.After(user.identity.Hosted.ExpiresAt) {
				t.Fatal("artifact grant outlives its session or permits caching")
			}
			if test.support != "" {
				var actor, effective, reason, audit string
				if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT actual_actor,effective_user,reason,event || route || project_id FROM hosted_audit WHERE session_id=? AND route LIKE '%/authorize' ORDER BY id DESC LIMIT 1", user.identity.Hosted.SessionID).Scan(&actor, &effective, &reason, &audit); err != nil {
					t.Fatal(err)
				}
				if actor != test.support || effective != user.identity.Subject || reason != "customer-request" || strings.Contains(audit, grant.Token) || strings.Contains(audit, user.token) || strings.Contains(audit, ref.ArtifactID) {
					t.Fatal("artifact audit must retain identities without content or capabilities")
				}
			}
		})
	}
}

func TestHostedArtifactGrantRevocation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		change func(*testing.T, hostedSecurityFixture, hostedSecurityUser)
	}{
		{name: "provider session revoked", change: func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			if err := f.provider.RevokeSession(t.Context(), user.identity.Hosted.SessionID); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "local session revoked", change: func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE hosted_sessions SET revoked_at=? WHERE token_hash=?", formatHubTime(time.Now()), apikey.HashToken(user.token)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "session expired", change: func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			f.provider.mu.Lock()
			defer f.provider.mu.Unlock()
			identity := f.provider.sessions[user.identity.Hosted.SessionID]
			identity.ExpiresAt = time.Now().Add(-time.Second)
			f.provider.sessions[identity.SessionID] = identity
		}},
		{name: "membership revoked", change: func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			f.provider.mu.Lock()
			defer f.provider.mu.Unlock()
			for id, member := range f.provider.members {
				if member.UserID == user.identity.Subject {
					delete(f.provider.members, id)
				}
			}
		}},
		{name: "project grant revoked", change: func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			if _, err := f.service.database.db.ExecContext(t.Context(), "DELETE FROM hosted_project_grants WHERE user_id=?", user.identity.Subject); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "organization changed", change: func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			f.provider.mu.Lock()
			defer f.provider.mu.Unlock()
			identity := f.provider.sessions[user.identity.Hosted.SessionID]
			identity.OrganizationID = "org_foreign"
			f.provider.sessions[identity.SessionID] = identity
		}},
		{name: "support permission removed", change: func(t *testing.T, f hostedSecurityFixture, _ hostedSecurityUser) {
			f.service.config.Hosted.SupportActors = nil
		}},
		{name: "missing session binding", change: func(t *testing.T, f hostedSecurityFixture, _ hostedSecurityUser) {
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE artifact_grants SET hosted_session_hash=''"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "different user session", change: func(t *testing.T, f hostedSecurityFixture, _ hostedSecurityUser) {
			other := f.user(t, "other", "viewer", "other@example.test", "read", "")
			if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE artifact_grants SET hosted_session_hash=?", apikey.HashToken(other.token)); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			ref, publisher, servicePath := seedHostedArtifact(t, f)
			user := f.user(t, "customer", "member", "customer@example.test", "write", "support@example.test")
			path := f.base + "/work-items/" + ref.WorkItemID + "/artifacts/" + ref.ArtifactID + "/access"
			response := f.request(t, user, http.MethodPost, path, map[string]int64{"revision": 1})
			requireNativeStatus(t, response, http.StatusOK)
			var grant artifact.Grant
			decodeHubResponse(t, response, &grant)
			request := artifact.ReadAuthorization{Token: grant.Token, ArtifactID: ref.ArtifactID, Revision: 1}
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, request), http.StatusNoContent)
			test.change(t, f, user)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, request), http.StatusNotFound)
		})
	}
}

func TestHostedArtifactPublisherBoundary(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	ref, publisher, servicePath := seedHostedArtifact(t, f)
	user := f.user(t, "customer", "viewer", "customer@example.test", "read", "")
	f.provider.mu.Lock()
	identity := f.provider.sessions[user.identity.Hosted.SessionID]
	identity.ExpiresAt = time.Now().Add(30 * time.Second)
	f.provider.sessions[identity.SessionID] = identity
	f.provider.mu.Unlock()
	path := f.base + "/work-items/" + ref.WorkItemID + "/artifacts/" + ref.ArtifactID + "/access"
	csrfRequest := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"revision":1}`))
	csrfRequest.Header.Set("Content-Type", "application/json")
	csrfRequest.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
	csrfResponse := httptest.NewRecorder()
	f.service.Handler().ServeHTTP(csrfResponse, csrfRequest)
	requireNativeStatus(t, csrfResponse, http.StatusForbidden)
	response := f.request(t, user, http.MethodPost, path, map[string]int64{"revision": 1})
	requireNativeStatus(t, response, http.StatusOK)
	var grant artifact.Grant
	decodeHubResponse(t, response, &grant)
	if grant.ExpiresAt.After(identity.ExpiresAt) {
		t.Fatal("grant must preserve the shorter provider expiry")
	}
	request := artifact.ReadAuthorization{Token: grant.Token, ArtifactID: ref.ArtifactID, Revision: 1}
	for _, test := range []struct {
		name, method, path string
	}{
		{"project content", http.MethodGet, f.base + "/work-items"},
		{"artifact references", http.MethodGet, f.base + "/work-items/" + ref.WorkItemID + "/artifacts"},
		{"artifact read grants", http.MethodPost, path},
		{"wrong method", http.MethodGet, servicePath + "/authorize"},
		{"wrong service", http.MethodPost, f.base + "/artifact-services/service_foreign/authorize"},
		{"wrong organization", http.MethodPost, strings.Replace(servicePath, "org_security", "org_foreign", 1) + "/authorize"},
		{"wrong project", http.MethodPost, strings.Replace(servicePath, string(f.project), "prj_foreign", 1) + "/authorize"},
		{"service binding", http.MethodPut, servicePath},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := performHubAPIRequest(t, f.service, test.method, test.path, publisher, request)
			if response.Code < http.StatusBadRequest {
				t.Fatalf("publisher gained access: status %d", response.Code)
			}
			if strings.Contains(response.Body.String(), "private-issue-sentinel") || strings.Contains(response.Body.String(), grant.Token) {
				t.Fatal("publisher denial leaked customer content or a capability")
			}
		})
	}
	for _, test := range []struct {
		name, statement string
	}{
		{"unscoped token", "UPDATE api_tokens SET native_only=0 WHERE id='artifact-publisher'"},
		{"admin token", "UPDATE api_tokens SET scope='admin' WHERE id='artifact-publisher'"},
		{"unbound token", "UPDATE artifact_services SET publisher_token_id=(SELECT principal_id FROM hosted_members LIMIT 1)"},
		{"revoked project", "DELETE FROM token_grants WHERE token_id='artifact-publisher'"},
		{"revoked publisher", "UPDATE api_tokens SET revoked_at=created_at WHERE id='artifact-publisher'"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newHostedSecurityFixture(t)
			ref, publisher, servicePath := seedHostedArtifact(t, f)
			user := f.user(t, "customer", "viewer", "customer@example.test", "read", "")
			path := f.base + "/work-items/" + ref.WorkItemID + "/artifacts/" + ref.ArtifactID + "/access"
			response := f.request(t, user, http.MethodPost, path, map[string]int64{"revision": 1})
			requireNativeStatus(t, response, http.StatusOK)
			var grant artifact.Grant
			decodeHubResponse(t, response, &grant)
			request := artifact.ReadAuthorization{Token: grant.Token, ArtifactID: ref.ArtifactID, Revision: 1}
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, request), http.StatusNoContent)
			if _, err := f.service.database.db.ExecContext(t.Context(), test.statement); err != nil {
				t.Fatal(err)
			}
			response = performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, request)
			if response.Code < http.StatusBadRequest {
				t.Fatalf("inactive publisher gained access: status %d", response.Code)
			}
		})
	}
}

func TestArtifactAuthorityReceiptsAndRevocation(t *testing.T) {
	t.Parallel()
	f := newNativeFixture(t, nil, "", "artifacts")
	approveHubTestPolicy(t, f.service, f.base+"/policy", hubTestPolicy())
	issue := f.create(t, "work")
	worker := f.worker(t, "worker")
	lease := claimNativeAttempt(t, f, worker, "machine", "session", issue.WorkItemID)
	start := nativeStartedEvent(lease)
	path := f.base + "/work-items/" + string(issue.WorkItemID)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/events", worker, start), 200)
	publisher := f.worker(t, "publisher")
	var publisherID string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT id FROM api_tokens WHERE token_hash=?", apikey.HashToken(publisher)).Scan(&publisherID); err != nil {
		t.Fatal(err)
	}
	binding := artifact.Binding{ServiceID: artifact.NewID("service"), Origin: "https://artifacts.example.com", Mode: "customer", PublisherTokenID: publisherID}
	servicePath := f.base + "/artifact-services/" + binding.ServiceID
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPut, servicePath, testHubAdminToken, binding), 200)
	response := performHubAPIRequest(t, f.service, http.MethodGet, f.base+"/artifact-services", f.token, nil)
	requireNativeStatus(t, response, 200)
	var bindings []artifact.Binding
	decodeHubResponse(t, response, &bindings)
	if len(bindings) != 1 || bindings[0].PublisherTokenID != "" {
		t.Fatal("publisher disclosed")
	}
	reservation := artifact.Reservation{ServiceID: binding.ServiceID, Mode: binding.Mode, HostedOptIn: binding.HostedOptIn, Scope: artifact.Scope{OrganizationID: string(f.project.OrganizationID), ProjectID: string(f.project.ID), WorkItemID: string(issue.WorkItemID), RunID: start.Data.RunID, AttemptID: start.Data.AttemptID}, LeaseID: string(lease.ID), FencingToken: int64(lease.FencingToken), Key: "log", Kind: "log", Bytes: 4 << 20}
	for _, test := range []struct {
		name  string
		edit  func(*artifact.Reservation)
		token string
		want  int
	}{
		{"current", func(*artifact.Reservation) {}, worker, 204},
		{"wrong service", func(r *artifact.Reservation) { r.ServiceID = artifact.NewID("service") }, worker, 404},
		{"wrong custody", func(r *artifact.Reservation) { r.Mode = "hosted" }, worker, 404},
		{"unapproved hosted consent", func(r *artifact.Reservation) { r.HostedOptIn = true }, worker, 404},
		{"operator cannot upload", func(*artifact.Reservation) {}, f.token, 403},
		{"publisher cannot upload", func(*artifact.Reservation) {}, publisher, 404},
		{"stale fence", func(r *artifact.Reservation) { r.FencingToken++ }, worker, 409},
		{"wrong attempt", func(r *artifact.Reservation) { r.AttemptID = artifact.NewID("attempt") }, worker, 404},
		{"wrong project", func(r *artifact.Reservation) { r.ProjectID = artifact.NewID("prj") }, worker, 404},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := reservation
			test.edit(&r)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/artifact-authority", test.token, r), test.want)
		})
	}
	now := time.Now().UTC()
	ref := artifact.Reference{SchemaVersion: 1, Scope: reservation.Scope, ServiceID: binding.ServiceID, ArtifactID: artifact.NewID("artifact"), ManifestID: artifact.NewID("manifest"), Revision: 1, SHA256: artifact.Digest([]byte("manifest")), Kind: "log", State: "partial", Availability: "available", Bytes: 100, Objects: 1, ExpiresAt: now.Add(time.Hour), ObservedAt: now}
	for _, test := range []struct {
		name, token string
		edit        func(*artifact.Reference)
		want        int
	}{
		{"publish", publisher, func(*artifact.Reference) {}, 204},
		{"retry", publisher, func(*artifact.Reference) {}, 204},
		{"worker forged receipt", worker, func(*artifact.Reference) {}, 404},
		{"changed receipt", publisher, func(r *artifact.Reference) { r.Bytes++ }, 409},
		{"foreign scope", publisher, func(r *artifact.Reference) { r.OrganizationID = artifact.NewID("org") }, 422},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := ref
			test.edit(&r)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/receipts", test.token, r), test.want)
		})
	}
	finish := start
	finish.Type = "run.finished"
	finish.IdempotencyKey = "finish"
	finish.Data.Sequence = 2
	finish.Data.Outcome = "succeeded"
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/events", worker, finish), 200)
	response = performHubAPIRequest(t, f.service, http.MethodGet, path+"/artifacts", f.token, nil)
	requireNativeStatus(t, response, 200)
	var refs []artifact.Reference
	decodeHubResponse(t, response, &refs)
	if len(refs) != 1 || refs[0] != ref {
		t.Fatal("references changed")
	}
	response = performHubAPIRequest(t, f.service, http.MethodPost, path+"/artifacts/"+ref.ArtifactID+"/access", f.token, map[string]int64{"revision": 1})
	requireNativeStatus(t, response, 200)
	var grant artifact.Grant
	decodeHubResponse(t, response, &grant)
	auth := artifact.ReadAuthorization{Token: grant.Token, ArtifactID: ref.ArtifactID, Revision: 1}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, auth), 204)
	for _, test := range []struct {
		name string
		edit func(*artifact.ReadAuthorization)
	}{{"wrong revision", func(r *artifact.ReadAuthorization) { r.Revision++ }}, {"wrong artifact", func(r *artifact.ReadAuthorization) { r.ArtifactID = artifact.NewID("artifact") }}, {"invalid token", func(r *artifact.ReadAuthorization) { r.Token = "invalid" }}} {
		t.Run(test.name, func(t *testing.T) {
			r := auth
			test.edit(&r)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, r), 404)
		})
	}
	var principal string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT id FROM api_tokens WHERE token_hash=?", apikey.HashToken(f.token)).Scan(&principal); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "DELETE FROM token_grants WHERE token_id=?", principal); err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, servicePath+"/authorize", publisher, auth), 404)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/artifacts/"+ref.ArtifactID+"/access", f.token, map[string]int64{"revision": 1}), 404)
}
