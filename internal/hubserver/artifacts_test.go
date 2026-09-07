package hubserver

import (
	"net/http"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/apikey"
	"github.com/digitaldrywood/detent/internal/artifact"
)

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
