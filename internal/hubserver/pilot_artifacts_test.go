package hubserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type pilotArtifactStorage struct{ objects sync.Map }

func (s *pilotArtifactStorage) Put(_ context.Context, key string, data []byte) (string, error) {
	if _, loaded := s.objects.LoadOrStore(key, bytes.Clone(data)); loaded {
		return "", artifact.ErrConflict
	}
	return "fixture-version", nil
}

func (s *pilotArtifactStorage) Get(_ context.Context, key, _ string, limit int64) ([]byte, error) {
	value, ok := s.objects.Load(key)
	if !ok {
		return nil, artifact.ErrMissing
	}
	data, ok := value.([]byte)
	if !ok || int64(len(data)) > limit {
		return nil, artifact.ErrIntegrity
	}
	return bytes.Clone(data), nil
}

func (s *pilotArtifactStorage) Delete(_ context.Context, key, _ string) error {
	s.objects.Delete(key)
	return nil
}

func pilotUploadArtifact(t *testing.T, f *browserHostedFixture, issue tracker.NativeIssue, runner runnerauth.Redemption) {
	t.Helper()
	organization := f.service.config.Hosted.OrganizationID
	base := "/api/v2/organizations/" + organization + "/projects/" + f.project
	response := f.page(t, "owner", base+"/policy")
	requireNativeStatus(t, response, http.StatusOK)
	var approved policy.Approval
	decodeHubResponse(t, response, &approved)
	response = performHubAPIRequest(t, f.service, http.MethodPost, base+"/claims", runner.Credential, tracker.NativeClaim{PolicyID: approved.Policy.ID, WorkItemID: issue.WorkItemID, MachineID: runner.MachineID, SessionID: "pilot-upload", TTLSeconds: 90, ProtocolMajor: 2, Capabilities: []string{"native_issues", "scoped_collaboration", tracker.NativeExecutionCapability}})
	requireNativeStatus(t, response, http.StatusOK)
	var lease tracker.NativeLease
	decodeHubResponse(t, response, &lease)
	start := nativeStartedEvent(lease)
	path := base + "/work-items/" + string(issue.WorkItemID)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/events", runner.Credential, start), http.StatusOK)

	publisher := "pilot-publisher-token"
	seedHubAPIToken(t, f.service, "pilot-publisher", publisher, apiScopeWorker)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{"UPDATE api_tokens SET native_only=1 WHERE id='pilot-publisher'", nil},
		{"INSERT INTO token_grants(token_id,organization_id,project_id) VALUES ('pilot-publisher',?,?)", []any{organization, f.project}},
	} {
		if _, err := f.service.database.db.ExecContext(t.Context(), statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	cfg := artifact.Config{ServiceID: artifact.NewID("service"), OrganizationID: organization, Mode: "customer", DatabasePath: filepath.Join(t.TempDir(), "catalog.db"), AllowedOrigins: []string{f.server.URL}, Policy: artifact.Policy{ID: "pilot", Limits: artifact.Limits{RetainedBytes: 8 << 20, ReservedBytes: 8 << 20, ArtifactBytes: 4 << 20, UploadBytes: 1 << 20, RetentionSeconds: 3600}, AbandonedUploadSeconds: 600, DeletionRecordSeconds: 7200, BackupSeconds: 3600}}
	service, err := artifact.NewService(t.Context(), cfg, &pilotArtifactStorage{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	remote := &artifact.RemoteHub{Origin: f.server.URL, OrganizationID: organization, ProjectID: f.project, ServiceID: cfg.ServiceID, PublisherToken: func() string { return publisher }}
	handler, err := artifact.NewHTTPServer(service, remote)
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(handler)
	t.Cleanup(gateway.Close)
	binding := artifact.Binding{ServiceID: cfg.ServiceID, Origin: gateway.URL, Mode: "customer", PublisherTokenID: "pilot-publisher"}
	raw, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO artifact_services(organization_id,project_id,id,binding_json,publisher_token_id) VALUES (?,?,?,?,'pilot-publisher')", organization, f.project, cfg.ServiceID, raw); err != nil {
		t.Fatal(err)
	}
	client := &artifact.Client{Origin: gateway.URL, Token: func(context.Context) (string, error) { return runner.Credential, nil }}
	upload, err := client.Reserve(t.Context(), artifact.Reservation{ServiceID: cfg.ServiceID, Mode: "customer", Scope: artifact.Scope{OrganizationID: organization, ProjectID: f.project, WorkItemID: string(issue.WorkItemID), RunID: start.Data.RunID, AttemptID: start.Data.AttemptID}, LeaseID: string(lease.ID), FencingToken: int64(lease.FencingToken), Key: "pilot-log", Kind: "log", Bytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("Pilot uploaded log remains readable with runners offline.\n<script>window.pilotArtifactExecuted=true</script>\n")
	if _, err := client.Append(t.Context(), upload.ArtifactID, artifact.Part{MediaType: "text/plain; charset=utf-8", SHA256: artifact.Digest(data), Data: data}); err != nil {
		t.Fatal(err)
	}
	ref, err := client.Finalize(t.Context(), upload.ArtifactID, "complete", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Publish(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	finish := start
	finish.Type, finish.IdempotencyKey, finish.Data.Sequence, finish.Data.Outcome = "run.finished", "finish", 2, "succeeded"
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/events", runner.Credential, finish), http.StatusOK)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, base+"/leases/"+string(lease.ID)+"/release", runner.Credential, tracker.NativeLeaseMutation{FencingToken: lease.FencingToken, Reason: "completed"}), http.StatusNoContent)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/artifact-authority", runner.Credential, upload.Reservation), http.StatusConflict)
}

func TestPilotHostedArtifactGatewayWithoutRunners(t *testing.T) {
	t.Parallel()
	f, issue, _ := pilotHostedJourney(t)
	pilotExpireRunners(t, f)
	response := f.page(t, "owner", issue+"/artifacts")
	requireNativeStatus(t, response, http.StatusOK)
	var refs []artifact.Reference
	decodeHubResponse(t, response, &refs)
	if len(refs) != 1 || refs[0].State != "complete" {
		t.Fatalf("uploaded references = %+v", refs)
	}
	ref := refs[0]
	access := issue + "/artifacts/" + ref.ArtifactID + "/access"
	for _, test := range []struct {
		account string
		want    int
	}{
		{"owner", http.StatusOK}, {"viewer", http.StatusOK},
		{"wrong-organization", http.StatusForbidden}, {"revoked", http.StatusUnauthorized},
	} {
		t.Run(test.account, func(t *testing.T) {
			response := f.setupRequest(t, test.account, http.MethodPost, access, map[string]int64{"revision": ref.Revision})
			requireNativeStatus(t, response, test.want)
			if test.want != http.StatusOK {
				return
			}
			var grant artifact.Grant
			decodeHubResponse(t, response, &grant)
			manifestPath := grant.Origin + "/v1/artifacts/" + ref.ArtifactID + "/manifests/" + strconv.FormatInt(ref.Revision, 10)
			raw := pilotArtifactDownload(t, manifestPath, grant.Token, http.StatusOK)
			if artifact.Digest(raw) != grant.SHA256 {
				t.Fatal("manifest does not match hosted receipt")
			}
			var manifest artifact.Manifest
			if err := json.Unmarshal(raw, &manifest); err != nil || len(manifest.Objects) != 1 {
				t.Fatalf("manifest objects = %d: %v", len(manifest.Objects), err)
			}
			object := manifest.Objects[0]
			raw = pilotArtifactDownload(t, manifestPath+"/objects/"+object.ID, grant.Token, http.StatusOK)
			if artifact.Digest(raw) != object.SHA256 || !strings.Contains(string(raw), "Pilot uploaded log remains readable") {
				t.Fatal("uploaded bytes changed")
			}
			pilotArtifactDownload(t, manifestPath, "invalid-grant", http.StatusForbidden)
		})
	}
	response = f.setupRequest(t, "viewer", http.MethodPost, access, map[string]int64{"revision": ref.Revision})
	requireNativeStatus(t, response, http.StatusOK)
	var grant artifact.Grant
	decodeHubResponse(t, response, &grant)
	requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_viewer"}, "project": {f.project}, "revoke": {"true"}}), http.StatusSeeOther)
	pilotArtifactDownload(t, grant.Origin+"/v1/artifacts/"+ref.ArtifactID+"/manifests/"+strconv.FormatInt(ref.Revision, 10), grant.Token, http.StatusForbidden)
}

func pilotArtifactDownload(t *testing.T, target, token string, want int) []byte {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, artifact.MaxManifestBytes))
	if err != nil || response.StatusCode != want {
		t.Fatalf("artifact download status=%d want=%d: %v", response.StatusCode, want, err)
	}
	return raw
}
