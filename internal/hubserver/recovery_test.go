package hubserver

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/digitaldrywood/detent/internal/runnerauth"
	"github.com/digitaldrywood/detent/internal/tracker"
)

const recoveryTestToken = "fresh-recovery-administrator-token-example"

func TestRecoveryPreservesCollaborationAndFencesAuthority(t *testing.T) {
	t.Parallel()
	f := newNativeFixture(t, nil, "", "portable")
	issue := f.create(t, "retained content")
	issuePath := f.base + "/work-items/" + string(issue.WorkItemID)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, issuePath+"/comments", f.token,
		tracker.CreateComment{Mutation: tracker.Mutation{IdempotencyKey: "comment"}, Body: "retained discussion"}), http.StatusOK)
	approveHubTestPolicy(t, f.service, f.base+"/policy", hubTestPolicy())
	worker := f.worker(t, "recovery-worker")
	lease := claimNativeAttempt(t, f, worker, "recovery-machine", "recovery-session", issue.WorkItemID)
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, issuePath+"/events", worker, nativeStartedEvent(lease)), http.StatusOK)
	runner := prepareRunner(t, f, runnerauth.Read, runnerauth.Claim)
	runner.enroll(t)
	pending := prepareRunner(t, f, runnerauth.Read)
	change := newChangeFixture(t, f.service)
	change.publish(t, "portable-version", "")
	changeBefore := change.detail(t)
	_, projectionID := seedProjection(t, f.service.database.db)
	var cursorKey []byte
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT cursor_key FROM hub_identity").Scan(&cursorKey); err != nil {
		t.Fatal(err)
	}
	preserved := map[string][]byte{}
	for _, suffix := range []string{"", "/comments", "/history"} {
		response := performHubAPIRequest(t, f.service, http.MethodGet, issuePath+suffix, f.token, nil)
		requireNativeStatus(t, response, http.StatusOK)
		preserved[suffix] = bytes.Clone(response.Body.Bytes())
	}
	sourcePath := f.service.database.path
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	sourceBefore, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "export.db")
	if err := BackupDatabase(t.Context(), sourcePath, backup); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "import.db")
	result, err := RestoreDatabase(t.Context(), backup, destination, []byte(recoveryTestToken))
	if err != nil {
		t.Fatal(err)
	}
	if result.SchemaVersion != supportedSchemaVersion || result.AdministratorID == "" {
		t.Fatalf("restore result = %+v", result)
	}
	restored := openTestService(t, Config{DatabasePath: destination})
	change.service, change.token = restored, recoveryTestToken
	if got := change.detail(t); !reflect.DeepEqual(got, changeBefore) {
		t.Fatalf("restored Change or artifact references changed: got %+v, want %+v", got, changeBefore)
	}
	for _, test := range []struct{ name, token string }{
		{"administrator", testHubAdminToken}, {"operator", f.token}, {"worker", worker}, {"runner", runner.redemption.Credential},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireNativeStatus(t, performHubAPIRequest(t, restored, http.MethodGet, "/health", test.token, nil), http.StatusUnauthorized)
		})
	}
	requireNativeStatus(t, performHubAPIRequest(t, restored, http.MethodGet, "/health", recoveryTestToken, nil), http.StatusOK)
	requireNativeStatus(t, performHubAPIRequest(t, restored, http.MethodPost, pending.base+"/runner-enrollments/redeem", pending.enrollment.Token, pending.redemption), http.StatusUnauthorized)
	for suffix, want := range preserved {
		response := performHubAPIRequest(t, restored, http.MethodGet, issuePath+suffix, recoveryTestToken, nil)
		requireNativeStatus(t, response, http.StatusOK)
		if !bytes.Equal(response.Body.Bytes(), want) {
			t.Fatalf("restored %s changed: %s", suffix, response.Body)
		}
	}
	for _, test := range []struct{ name, query, want string }{
		{"released leases", "SELECT count(*) FROM leases WHERE released_at IS NULL", "0"},
		{"preserved runner identities", "SELECT count(*) FROM runner_identities", "1"},
		{"preserved principal", "SELECT count(*) FROM api_tokens WHERE id = 'bootstrap-admin' AND revoked_at IS NOT NULL", "1"},
		{"external identity", fmt.Sprintf("SELECT github_node_id FROM issues WHERE id = %d", projectionID), "I_issue"},
		{"foreign keys", "SELECT count(*) FROM pragma_foreign_key_check", "0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var got string
			if err := restored.database.db.QueryRowContext(t.Context(), test.query).Scan(&got); err != nil || got != test.want {
				t.Fatalf("got %q, want %q: %v", got, test.want, err)
			}
		})
	}
	var restoredKey []byte
	if err := restored.database.db.QueryRowContext(t.Context(), "SELECT cursor_key FROM hub_identity").Scan(&restoredKey); err != nil || bytes.Equal(cursorKey, restoredKey) {
		t.Fatalf("cursor authority was not rotated: %v", err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyDatabase(t.Context(), destination); err != nil {
		t.Fatal(err)
	}
	sourceAfter, err := os.ReadFile(sourcePath)
	if err != nil || !bytes.Equal(sourceBefore, sourceAfter) {
		t.Fatalf("backup changed source database: %v", err)
	}
}

func TestRecoveryRejectsUnsafeSourcesAndDestinations(t *testing.T) {
	t.Parallel()
	for _, test := range []string{"missing", "empty", "future", "foreign database", "active owner", "existing destination", "same path", "reused credential", "short credential", "cancelled"} {
		t.Run(test, func(t *testing.T) {
			t.Parallel()
			source := filepath.Join(t.TempDir(), "source.db")
			s := openTestService(t, Config{DatabasePath: source})
			if test != "active owner" {
				if err := s.Close(); err != nil {
					t.Fatal(err)
				}
			}
			destination := filepath.Join(t.TempDir(), "restored.db")
			token := recoveryTestToken
			ctx := t.Context()
			switch test {
			case "missing":
				source += ".missing"
			case "empty":
				source += ".empty"
				if err := os.WriteFile(source, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			case "future", "foreign database":
				db, err := sql.Open("sqlite", source)
				if err != nil {
					t.Fatal(err)
				}
				statement := fmt.Sprintf("INSERT INTO hub_schema_version(version_id,is_applied) VALUES (%d,1)", supportedSchemaVersion+1)
				if test == "foreign database" {
					statement = "PRAGMA application_id = 123"
				}
				if _, err := db.ExecContext(ctx, statement); err != nil {
					t.Fatal(err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
			case "existing destination":
				if err := os.WriteFile(destination, []byte("preserve"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "same path":
				destination = source
			case "reused credential":
				token = testHubAdminToken
			case "short credential":
				token = "short"
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before, beforeErr := os.ReadFile(destination)
			if _, err := RestoreDatabase(ctx, source, destination, []byte(token)); err == nil {
				t.Fatal("unsafe restore succeeded")
			}
			after, afterErr := os.ReadFile(destination)
			if !reflect.DeepEqual(before, after) || (beforeErr == nil) != (afterErr == nil) {
				t.Fatal("failed restore published or changed destination")
			}
			staging, err := filepath.Glob(filepath.Join(filepath.Dir(destination), ".hub-restore-*"))
			if err != nil || len(staging) != 0 {
				t.Fatalf("failed restore left staging files: %v, %v", staging, err)
			}
		})
	}
}

func TestSelfHostedRunnerVersionCompatibility(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		version      string
		major        int
		capabilities []string
		want         int
	}{
		{"older feature set", "older-release", 2, []string{"native_issues", "scoped_collaboration"}, http.StatusOK},
		{"current feature set", "current-release", 2, []string{"native_issues", "scoped_collaboration", tracker.NativeExecutionCapability}, http.StatusOK},
		{"unknown required feature", "future-release", 2, []string{"native_issues", "scoped_collaboration", "future_required"}, http.StatusUnprocessableEntity},
		{"unsupported major", "future-release", 99, []string{"native_issues", "scoped_collaboration"}, http.StatusUnprocessableEntity},
		{"missing required feature", "older-release", 2, []string{"native_issues"}, http.StatusUnprocessableEntity},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newNativeFixture(t, nil, "", "compatibility")
			issue := f.create(t, "work")
			approveHubTestPolicy(t, f.service, f.base+"/policy", hubTestPolicy())
			r := prepareRunner(t, f, runnerauth.Read, runnerauth.Claim)
			r.redemption.Version = test.version
			r.enroll(t)
			claim := tracker.NativeClaim{PolicyID: hubTestPolicy().ID, WorkItemID: issue.WorkItemID,
				MachineID: r.binding.MachineID, SessionID: "version-session", TTLSeconds: 90,
				ProtocolMajor: test.major, Capabilities: test.capabilities}
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, f.base+"/claims", r.redemption.Credential, claim), test.want)
		})
	}
}
