package hubserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestHostedIndependentChangeChecks(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	for _, test := range []struct {
		name, project string
		review        bool
	}{
		{"human review", f.project, true},
		{"automatic", f.privateProject, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := "/api/v2/organizations/org_browser_preview/projects/" + test.project
			requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {test.project}, "write": {"true"}}), http.StatusSeeOther)
			descriptor := hubTestPolicy()
			descriptor.ConfigDigest = policy.Digest([]byte(test.project))
			descriptor.Gates.Kind = "command"
			if test.review {
				descriptor.Gates.Kind = "human_review"
			}
			descriptor.Gates.RequiredChecks, descriptor.Gates.Validator, descriptor.Gates.AutomatedReview = 1, true, ""
			if !test.review {
				descriptor.Gates.AutomatedReview = "off"
				descriptor.Gates.AutoPromote = true
			}
			descriptor = descriptor.WithID()
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding/policy", policy.Change{Policy: descriptor}), http.StatusOK)
			principal, token := "ci-"+test.project, "credential-"+test.project
			seedHubAPIToken(t, f.service, principal, token, apiScopeOperator)
			if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants VALUES (?, 'org_browser_preview', ?)", principal, test.project); err != nil {
				t.Fatal(err)
			}
			rules := tracker.ChangeReviewPolicy{PolicyID: descriptor.ID, RequireReview: test.review, RequiredChecks: []tracker.ChangeCheckSpec{{Name: "test", PrincipalID: principal, WorkflowID: "ci.yml", WorkflowSHA256: policy.Digest([]byte("trusted CI")), Source: "independent", MaxAgeSeconds: 3600}}}
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "rules"}, Policy: rules}), http.StatusOK)
			response := f.setupRequest(t, "owner", http.MethodPost, base+"/work-items", tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "work"}, Title: "Independent CI", State: "Todo"})
			requireNativeStatus(t, response, http.StatusOK)
			var issue tracker.NativeIssue
			decodeHubResponse(t, response, &issue)
			path := base + "/work-items/" + string(issue.WorkItemID) + "/changes"
			response = f.setupRequest(t, "owner", http.MethodPost, path, tracker.CreateChange{Mutation: tracker.Mutation{IdempotencyKey: "change"}, Title: "Independent CI"})
			requireNativeStatus(t, response, http.StatusOK)
			var change tracker.ChangeRequest
			decodeHubResponse(t, response, &change)
			path += "/" + change.ID
			input := changeTestInput()
			input.PolicyID = descriptor.ID
			input.External = &tracker.ChangeExternalReference{Provider: "github", ID: "123", URL: "https://github.com/example/repo/pull/123"}
			response = f.setupRequest(t, "owner", http.MethodPost, path+"/versions", tracker.PublishChangeVersion{Mutation: tracker.Mutation{IdempotencyKey: "publish"}, ChangeVersionInput: input})
			requireNativeStatus(t, response, http.StatusOK)
			var version tracker.ChangeVersion
			decodeHubResponse(t, response, &version)
			checkPath := path + "/versions/" + version.ID + "/checks"
			detail := func() tracker.ChangeDetail {
				t.Helper()
				response := f.setupRequest(t, "owner", http.MethodGet, path, nil)
				requireNativeStatus(t, response, http.StatusOK)
				var result tracker.ChangeDetail
				decodeHubResponse(t, response, &result)
				return result
			}
			if summary := detail().Summary; summary.Checks != "missing" || summary.Status != "needs_evidence" {
				t.Fatalf("before checks: %+v", summary)
			}
			testHostedCheckBoundaries(t, f, base, checkPath, principal, token, version)
			testHostedCheckRevocation(t, f, base, checkPath, principal, token, version, false)
			for _, invalid := range []struct {
				name string
				edit func(*tracker.SubmitChangeCheck)
				want int
			}{
				{"head", func(r *tracker.SubmitChangeCheck) { r.HeadSHA = strings.Repeat("f", 40) }, 422},
				{"run", func(r *tracker.SubmitChangeCheck) { r.RunID = "wrong" }, 422},
				{"check run", func(r *tracker.SubmitChangeCheck) { r.CheckRunID = "wrong" }, 422},
				{"policy", func(r *tracker.SubmitChangeCheck) { r.PolicyID = "wrong" }, 422},
				{"config", func(r *tracker.SubmitChangeCheck) { r.ConfigDigest = "wrong" }, 422},
				{"workflow", func(r *tracker.SubmitChangeCheck) { r.WorkflowID = "wrong" }, 422},
				{"workflow digest", func(r *tracker.SubmitChangeCheck) { r.WorkflowSHA256 = "wrong" }, 422},
				{"source", func(r *tracker.SubmitChangeCheck) { r.Source = "customer" }, 404},
				{"old completion", func(r *tracker.SubmitChangeCheck) { r.CompletedAt = version.CreatedAt.Add(-time.Second) }, 422},
				{"future completion", func(r *tracker.SubmitChangeCheck) { r.CompletedAt = time.Now().Add(time.Hour) }, 422},
			} {
				t.Run(invalid.name, func(t *testing.T) {
					request := changeTestResult(version)
					request.IdempotencyKey = invalid.name
					invalid.edit(&request)
					requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, checkPath, token, request), invalid.want)
				})
			}
			request := changeTestResult(version)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, checkPath, token, request), http.StatusOK)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, checkPath, token, request), http.StatusOK)
			testHostedCheckRevocation(t, f, base, checkPath, principal, token, version, true)
			request.Conclusion = "failure"
			for _, key := range []string{"check", "changed-result"} {
				request.IdempotencyKey = key
				requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, checkPath, token, request), http.StatusConflict)
			}
			current := detail()
			if len(current.Checks) != 1 || current.Summary.Checks != "success" {
				t.Fatalf("accepted checks: %+v", current)
			}
			if test.review {
				if current.Summary.Status != "needs_evidence" || current.Summary.NativeReview != "pending" {
					t.Fatalf("review bypassed: %+v", current.Summary)
				}
				requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path+"/versions/"+version.ID+"/reviews", tracker.ReviewChange{Mutation: tracker.Mutation{IdempotencyKey: "approve"}, Decision: "approved"}), http.StatusOK)
			}
			if summary := detail().Summary; summary.Status != "reviewed" || summary.ExternalReview != "external_gate" {
				t.Fatalf("readiness or external protection changed: %+v", summary)
			}
			now := f.service.config.now
			f.service.config.now = func() time.Time { return version.CreatedAt.Add(time.Hour) }
			if summary := detail().Summary; summary.Checks != "stale" || summary.Status != "needs_evidence" {
				t.Fatalf("freshness bypassed: %+v", summary)
			}
			f.service.config.now = now
		})
	}
}

func testHostedCheckBoundaries(t *testing.T, f *browserHostedFixture, base, path, principal, token string, version tracker.ChangeVersion) {
	t.Helper()
	for _, scope := range []apiScope{apiScopeWorker, apiScopeOperator, apiScopeAdmin} {
		id := principal + "-" + string(scope)
		seedHubAPIToken(t, f.service, id, id, scope)
		if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants VALUES (?, 'org_browser_preview', ?)", id, strings.TrimPrefix(base, "/api/v2/organizations/org_browser_preview/projects/")); err != nil {
			t.Fatal(err)
		}
		t.Run("unapproved "+string(scope), func(t *testing.T) {
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, id, changeTestResult(version)), http.StatusNotFound)
		})
	}
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path, changeTestResult(version)), http.StatusNotFound)
	for _, test := range []struct{ name, method, path string }{
		{"change read", http.MethodGet, strings.Split(path, "/versions/")[0]},
		{"review", http.MethodPost, strings.TrimSuffix(path, "checks") + "reviews"},
		{"publish", http.MethodPost, strings.Split(path, "/versions/")[0] + "/versions"},
		{"policy", http.MethodPut, base + "/change-review-policy"},
		{"token administration", http.MethodPost, "/api/v1/tokens"},
		{"grant administration", http.MethodPost, "/api/v2/tokens/" + principal + "/grants"},
		{"wrong organization", http.MethodPost, strings.Replace(path, "org_browser_preview", "org_other", 1)},
		{"wrong project", http.MethodPost, strings.Replace(path, base, "/api/v2/organizations/org_browser_preview/projects/prj_unknown", 1)},
		{"wrong item", http.MethodPost, strings.Replace(path, "/work-items/", "/work-items/other", 1)},
		{"wrong version", http.MethodPost, strings.Replace(path, version.ID, "version_other", 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireNativeStatus(t, performHubAPIRequest(t, f.service, test.method, test.path, token, changeTestResult(version)), http.StatusNotFound)
		})
	}
}

func testHostedCheckRevocation(t *testing.T, f *browserHostedFixture, base, path, principal, token string, version tracker.ChangeVersion, replay bool) {
	t.Helper()
	ctx := t.Context()
	project := strings.TrimPrefix(base, "/api/v2/organizations/org_browser_preview/projects/")
	var rules string
	if err := f.service.database.db.QueryRowContext(ctx, "SELECT policy_json FROM change_review_policies WHERE project_id=?", project).Scan(&rules); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, statement, restore string
		args, restoreArgs        []any
		want                     int
	}{
		{"revoked", "UPDATE api_tokens SET revoked_at=created_at WHERE id=?", "UPDATE api_tokens SET revoked_at=NULL WHERE id=?", []any{principal}, []any{principal}, 401},
		{"rotated", "UPDATE api_tokens SET token_hash='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' WHERE id=?", "UPDATE api_tokens SET token_hash=? WHERE id=?", []any{principal}, nil, 401},
		{"expired", "UPDATE api_tokens SET expires_at=created_at WHERE id=?", "UPDATE api_tokens SET expires_at=NULL WHERE id=?", []any{principal}, []any{principal}, 401},
		{"role changed", "UPDATE api_tokens SET scope='admin' WHERE id=?", "UPDATE api_tokens SET scope='operator' WHERE id=?", []any{principal}, []any{principal}, 404},
		{"project grant", "DELETE FROM token_grants WHERE token_id=? AND project_id=?", "INSERT INTO token_grants VALUES (?, 'org_browser_preview', ?)", []any{principal, project}, []any{principal, project}, 404},
		{"policy principal", "UPDATE change_review_policies SET policy_json=json_set(policy_json,'$.required_checks[0].principal_id','other') WHERE project_id=?", "UPDATE change_review_policies SET policy_json=? WHERE project_id=?", []any{project}, []any{rules, project}, 404},
		{"policy source", "UPDATE change_review_policies SET policy_json=json_set(policy_json,'$.required_checks[0].source','customer') WHERE project_id=?", "UPDATE change_review_policies SET policy_json=? WHERE project_id=?", []any{project}, []any{rules, project}, 404},
		{"repository policy", "UPDATE change_review_policies SET policy_json=json_set(policy_json,'$.policy_id','other') WHERE project_id=?", "UPDATE change_review_policies SET policy_json=? WHERE project_id=?", []any{project}, []any{rules, project}, 404},
	} {
		for _, during := range []bool{false, true} {
			name := test.name + "/admission"
			if during {
				name = test.name + "/transaction"
			}
			if replay {
				name += "/replay"
			}
			t.Run(name, func(t *testing.T) {
				if test.name == "rotated" {
					var hash string
					if err := f.service.database.db.QueryRowContext(ctx, "SELECT token_hash FROM api_tokens WHERE id=?", principal).Scan(&hash); err != nil {
						t.Fatal(err)
					}
					test.restoreArgs = []any{hash, principal}
				}
				mutate := func() {
					if _, err := f.service.database.db.ExecContext(ctx, test.statement, test.args...); err != nil {
						t.Fatal(err)
					}
				}
				t.Cleanup(func() {
					if _, err := f.service.database.db.ExecContext(ctx, test.restore, test.restoreArgs...); err != nil {
						t.Error(err)
					}
				})
				raw, err := json.Marshal(changeTestResult(version))
				if err != nil {
					t.Fatal(err)
				}
				body := &hostedChangePolicyReader{Reader: strings.NewReader(string(raw))}
				if during {
					body.beforeRead = mutate
				} else {
					mutate()
				}
				request := httptest.NewRequest(http.MethodPost, path, body)
				request.Header.Set("Authorization", "Bearer "+token)
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				f.service.Handler().ServeHTTP(response, request)
				want := test.want
				if during {
					want = http.StatusNotFound
				}
				requireNativeStatus(t, response, want)
				if body.beforeRead != nil {
					t.Fatal("transaction revocation was not exercised")
				}
			})
		}
	}
}
