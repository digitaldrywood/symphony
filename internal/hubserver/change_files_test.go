package hubserver

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func seedReviewBundle(t *testing.T, f changeFixture, version tracker.ChangeVersion, expires time.Time) tracker.ChangeReviewBundle {
	t.Helper()
	ref := artifact.Reference{Scope: artifact.Scope{OrganizationID: string(f.project.OrganizationID), ProjectID: string(f.project.ID), WorkItemID: string(f.issue.WorkItemID), VersionID: version.ID, RunID: version.RunID, AttemptID: version.AttemptID}, ArtifactID: artifact.NewID("artifact"), ManifestID: artifact.NewID("manifest"), ServiceID: artifact.NewID("service"), Revision: 1, SHA256: version.Code.SHA256, Kind: "diff", State: "complete", Availability: "available", ExpiresAt: expires}
	raw, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO artifact_services(organization_id,project_id,id,binding_json,publisher_token_id) VALUES(?,?,?,'{}',?)", ref.OrganizationID, ref.ProjectID, ref.ServiceID, version.Checks[0].PrincipalID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO artifact_references(organization_id,project_id,work_item_id,service_id,artifact_id,revision,manifest_id,reference_json) VALUES(?,?,?,?,?,?,?,?)", ref.OrganizationID, ref.ProjectID, ref.WorkItemID, ref.ServiceID, ref.ArtifactID, ref.Revision, ref.ManifestID, raw); err != nil {
		t.Fatal(err)
	}
	return tracker.ChangeReviewBundle{ArtifactID: ref.ArtifactID, Revision: ref.Revision, SHA256: ref.SHA256, HeadSHA: version.HeadSHA}
}

func TestBoundReviewDecisions(t *testing.T) {
	t.Parallel()
	f := newChangeFixture(t, openTestService(t, Config{DatabasePath: filepath.Join(t.TempDir(), "hub.db")}))
	first := f.publish(t, "first", "")
	bundle := seedReviewBundle(t, f, first, time.Now().Add(time.Hour))
	path := f.path + "/versions/" + first.ID + "/reviews"
	for _, tt := range []struct {
		name string
		edit func(*tracker.ReviewChange)
		want int
	}{
		{"valid", func(*tracker.ReviewChange) {}, 200},
		{"wrong head", func(r *tracker.ReviewChange) { r.Bundle.HeadSHA = strings.Repeat("c", 40) }, 422},
		{"wrong manifest", func(r *tracker.ReviewChange) { r.Bundle.SHA256 = strings.Repeat("d", 64) }, 422},
		{"wrong artifact", func(r *tracker.ReviewChange) { r.Bundle.ArtifactID = artifact.NewID("artifact") }, 404},
		{"wrong revision", func(r *tracker.ReviewChange) { r.Bundle.Revision++ }, 404},
		{"wrong expected version", func(r *tracker.ReviewChange) { r.ExpectedVersionID = artifact.NewID("version") }, 409},
		{"invalid decision", func(r *tracker.ReviewChange) { r.Decision = "merge" }, 422},
	} {
		t.Run(tt.name, func(t *testing.T) {
			copy := bundle
			r := tracker.ReviewChange{Mutation: tracker.Mutation{IdempotencyKey: tt.name}, Decision: "approved", ExpectedVersionID: first.ID, Bundle: &copy}
			tt.edit(&r)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, f.token, r), tt.want)
		})
	}
	second := f.publish(t, "second", first.ID)
	for _, tt := range []struct {
		key  string
		want int
	}{{"valid", 200}, {"stale-tab", 409}} {
		r := tracker.ReviewChange{Mutation: tracker.Mutation{IdempotencyKey: tt.key}, Decision: "approved", ExpectedVersionID: first.ID, Bundle: &bundle}
		requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, f.token, r), tt.want)
	}
	r := tracker.ReviewChange{Mutation: tracker.Mutation{IdempotencyKey: "rebind"}, Decision: "approved", ExpectedVersionID: second.ID, Bundle: &bundle}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, f.path+"/versions/"+second.ID+"/reviews", f.token, r), 422)
	if detail := f.detail(t); detail.Summary.NativeReview != "stale" || len(detail.Reviews) != 1 {
		t.Fatalf("stale approval transferred: %#v", detail.Summary)
	}
	worker := f.worker(t, "review-worker")
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, worker, r), 403)
}

func TestViewedFilesVersionActorAndReplay(t *testing.T) {
	t.Parallel()
	config := Config{DatabasePath: filepath.Join(t.TempDir(), "hub.db")}
	f := newChangeFixture(t, openTestService(t, config))
	version := f.publish(t, "first", "")
	bundle := seedReviewBundle(t, f, version, time.Now().Add(time.Hour))
	path := f.path + "/versions/" + version.ID + "/viewed-files"
	r := tracker.ViewChangeFile{Mutation: tracker.Mutation{IdempotencyKey: "view"}, Bundle: bundle, FileSHA256: strings.Repeat("f", 64), Viewed: true}
	for _, tt := range []struct {
		name string
		edit func(*tracker.ViewChangeFile)
		want int
	}{
		{"view", func(*tracker.ViewChangeFile) {}, 200},
		{"retry", func(*tracker.ViewChangeFile) {}, 200},
		{"changed replay", func(r *tracker.ViewChangeFile) { r.Viewed = false }, 409},
		{"raw filename", func(r *tracker.ViewChangeFile) { r.IdempotencyKey = "path"; r.FileSHA256 = "private.go" }, 422},
		{"wrong bundle", func(r *tracker.ViewChangeFile) {
			r.IdempotencyKey = "bundle"
			r.Bundle.SHA256 = strings.Repeat("e", 64)
		}, 422},
	} {
		t.Run(tt.name, func(t *testing.T) {
			copy := r
			tt.edit(&copy)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, f.token, copy), tt.want)
		})
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service = openTestService(t, config)
	response := performHubAPIRequest(t, f.service, http.MethodGet, path, f.token, nil)
	requireNativeStatus(t, response, 200)
	var files []tracker.ChangeViewedFile
	decodeHubResponse(t, response, &files)
	if len(files) != 1 || !files[0].Viewed || files[0].ManifestSHA256 != bundle.SHA256 {
		t.Fatalf("viewed state lost: %#v", files)
	}
	second := f.publish(t, "second", version.ID)
	response = performHubAPIRequest(t, f.service, http.MethodGet, f.path+"/versions/"+second.ID+"/viewed-files", f.token, nil)
	requireNativeStatus(t, response, 200)
	decodeHubResponse(t, response, &files)
	if len(files) != 0 {
		t.Fatal("viewed state transferred to new version")
	}
	other := newNativeFixture(t, f.service, "", "other-reviewer")
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodGet, path, other.token, nil), 404)
	var otherPrincipal string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT id FROM api_tokens WHERE name='operator-other-reviewer'").Scan(&otherPrincipal); err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, "/api/v2/tokens/"+otherPrincipal+"/grants", testHubAdminToken, map[string]string{"organization_id": string(f.project.OrganizationID), "project_id": string(f.project.ID)}), 204)
	response = performHubAPIRequest(t, f.service, http.MethodGet, path, other.token, nil)
	requireNativeStatus(t, response, 200)
	decodeHubResponse(t, response, &files)
	if len(files) != 0 {
		t.Fatal("viewed state transferred to another project member")
	}
	expired := seedReviewBundle(t, f, second, time.Now().Add(-time.Hour))
	r.Bundle = expired
	r.IdempotencyKey = "expired"
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, f.path+"/versions/"+second.ID+"/viewed-files", f.token, r), 422)
}
