package hubserver

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/changerequest"
	"github.com/digitaldrywood/detent/internal/onboarding"
	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestHostedChangePolicyAuthorization(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, role, grant, email string
		runner                   bool
		want                     int
	}{
		{"owner", "owner", "write", "owner@example.test", true, http.StatusOK},
		{"admin without runner grants", "admin", "write", "admin@example.test", false, http.StatusOK},
		{"owner without runner grants", "owner", "write", "owner@example.test", false, http.StatusOK},
		{"member", "member", "write", "member@example.test", true, http.StatusNotFound},
		{"viewer", "viewer", "write", "viewer@example.test", true, http.StatusNotFound},
		{"read only owner", "owner", "read", "owner@example.test", true, http.StatusNotFound},
		{"ungranted owner", "owner", "", "owner@example.test", false, http.StatusNotFound},
		{"staff owner", "owner", "write", "staff@example.test", true, http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			owner := f.user(t, "setup", "owner", "setup@example.test", "write", "")
			requireNativeStatus(t, f.request(t, owner, http.MethodPut, f.base+"/onboarding/policy", policy.Change{Policy: hubTestPolicy()}), http.StatusOK)
			user := f.user(t, "approver", test.role, test.email, test.grant, "")
			if test.grant != "" {
				f.grant(t, user, test.grant == "write", test.runner)
			}
			request := tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "approve"}, Policy: tracker.ChangeReviewPolicy{PolicyID: hubTestPolicy().ID, RequireReview: true}}
			requireNativeStatus(t, f.request(t, user, http.MethodPut, f.base+"/change-review-policy", request), test.want)
			var count int
			if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM change_review_policies").Scan(&count); err != nil {
				t.Fatal(err)
			}
			if (count == 1) != (test.want == http.StatusOK) {
				t.Fatalf("stored policies = %d, response status = %d", count, test.want)
			}
		})
	}
}

func TestHostedChangePolicyJourney(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	seedHubAPIToken(t, f.service, "ungranted-ci", "ungranted-ci-token", apiScopeOperator)
	for _, project := range []string{f.project, f.privateProject} {
		t.Run(project, func(t *testing.T) {
			base := "/api/v2/organizations/org_browser_preview/projects/" + project
			requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {project}, "write": {"true"}, "runner": {"true"}}), http.StatusSeeOther)
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding", map[string]any{"idempotency_key": "setup", "progress": onboarding.Progress{Repository: "existing"}}), http.StatusOK)
			descriptor := hubTestPolicy()
			descriptor.ConfigDigest = policy.Digest([]byte(project))
			descriptor.Gates.Kind, descriptor.Gates.RequiredChecks, descriptor.Gates.Validator = "human_review", 1, true
			descriptor.Gates.AutomatedReview = ""
			descriptor = descriptor.WithID()
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding/policy", policy.Change{Policy: descriptor}), http.StatusOK)
			principal := "ci-" + project
			seedHubAPIToken(t, f.service, principal, "credential-"+project, apiScopeOperator)
			if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES (?,'org_browser_preview',?)", principal, project); err != nil {
				t.Fatal(err)
			}
			rules := tracker.ChangeReviewPolicy{PolicyID: descriptor.ID, RequireReview: true, RequiredChecks: []tracker.ChangeCheckSpec{{Name: "test", PrincipalID: principal, WorkflowID: "ci.yml", WorkflowSHA256: policy.Digest([]byte("trusted CI")), Source: "independent", MaxAgeSeconds: 3600}}}
			for _, test := range []struct {
				name string
				edit func(*tracker.ChangeReviewPolicy)
			}{
				{"review floor", func(p *tracker.ChangeReviewPolicy) { p.RequireReview = false }},
				{"check floor", func(p *tracker.ChangeReviewPolicy) { p.RequiredChecks = nil }},
				{"validator floor", func(p *tracker.ChangeReviewPolicy) { p.RequiredChecks[0].Source = "customer" }},
				{"unpinned workflow", func(p *tracker.ChangeReviewPolicy) { p.RequiredChecks[0].WorkflowSHA256 = "" }},
				{"ungranted principal", func(p *tracker.ChangeReviewPolicy) { p.RequiredChecks[0].PrincipalID = "ungranted-ci" }},
				{"stale repository policy", func(p *tracker.ChangeReviewPolicy) { p.PolicyID = hubTestPolicy().ID }},
			} {
				t.Run(test.name, func(t *testing.T) {
					invalid := rules
					invalid.RequiredChecks = append([]tracker.ChangeCheckSpec(nil), rules.RequiredChecks...)
					test.edit(&invalid)
					requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: test.name}, Policy: invalid}), http.StatusUnprocessableEntity)
				})
			}
			approve := tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "approve"}, Policy: rules}
			approve.Policy.ID = "client-supplied-identity"
			response := f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", approve)
			requireNativeStatus(t, response, http.StatusOK)
			decodeHubResponse(t, response, &rules)
			if rules.ID != changerequest.PolicyID(rules) || rules.ID == approve.Policy.ID {
				t.Fatalf("policy identity = %q", rules.ID)
			}
			replay := f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", approve)
			requireNativeStatus(t, replay, http.StatusOK)
			if replay.Body.String() != response.Body.String() {
				t.Fatal("policy retry changed the approval")
			}
			approve.IdempotencyKey = "stale-expected"
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", approve), http.StatusConflict)
			response = f.setupRequest(t, "owner", http.MethodPost, base+"/work-items", tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "work"}, Title: "Publish a Change", State: "Todo"})
			requireNativeStatus(t, response, http.StatusOK)
			var issue tracker.NativeIssue
			decodeHubResponse(t, response, &issue)
			path := base + "/work-items/" + string(issue.WorkItemID) + "/changes"
			response = f.setupRequest(t, "owner", http.MethodPost, path, tracker.CreateChange{Mutation: tracker.Mutation{IdempotencyKey: "change"}, Title: "Native Change"})
			requireNativeStatus(t, response, http.StatusOK)
			var change tracker.ChangeRequest
			decodeHubResponse(t, response, &change)
			path += "/" + change.ID
			publish := tracker.PublishChangeVersion{Mutation: tracker.Mutation{IdempotencyKey: "publish"}, ChangeVersionInput: changeTestInput()}
			publish.PolicyID = descriptor.ID
			response = f.setupRequest(t, "owner", http.MethodPost, path+"/versions", publish)
			requireNativeStatus(t, response, http.StatusOK)
			var version tracker.ChangeVersion
			decodeHubResponse(t, response, &version)
			if version.PolicyID != descriptor.ID || version.ReviewPolicy.ID != rules.ID || len(version.Checks) != 1 || version.Checks[0].PrincipalID != principal {
				t.Fatalf("published policy snapshot = %+v", version)
			}
			approve.IdempotencyKey, approve.ExpectedID = "update", rules.ID
			approve.Policy.RequiredChecks[0].MaxAgeSeconds = 7200
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", approve), http.StatusOK)
			response = f.setupRequest(t, "owner", http.MethodGet, path, nil)
			requireNativeStatus(t, response, http.StatusOK)
			var detail tracker.ChangeDetail
			decodeHubResponse(t, response, &detail)
			if len(detail.Versions) != 1 || detail.Versions[0].ReviewPolicy.ID != rules.ID || detail.Summary.Status != "stale_policy" {
				t.Fatalf("changed policy rewrote published version or was not stale: %+v", detail)
			}
			updated := descriptor
			updated.ConfigDigest = policy.Digest([]byte("updated-" + project))
			updated = updated.WithID()
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding/policy", policy.Change{ExpectedID: descriptor.ID, Policy: updated}), http.StatusOK)
			publish.IdempotencyKey, publish.ExpectedVersionID = "stale-publish", version.ID
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path+"/versions", publish), http.StatusConflict)
			publish.IdempotencyKey, publish.PolicyID = "stale-review-policy", updated.ID
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path+"/versions", publish), http.StatusConflict)
		})
	}
	first := "/api/v2/organizations/org_browser_preview/projects/" + f.project
	second := "/api/v2/organizations/org_browser_preview/projects/" + f.privateProject
	response := f.setupRequest(t, "owner", http.MethodGet, second+"/change-review-policy", nil)
	requireNativeStatus(t, response, http.StatusOK)
	var secondPolicy tracker.ChangeReviewPolicy
	decodeHubResponse(t, response, &secondPolicy)
	requireNativeStatus(t, f.form(t, "owner", "/organization/grants", url.Values{"user": {"user_browser_owner"}, "project": {f.project}, "revoke": {"true"}}), http.StatusSeeOther)
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		t.Run("revoked project/"+method, func(t *testing.T) {
			requireNativeStatus(t, f.setupRequest(t, "owner", method, first+"/change-review-policy", tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "cross-project"}, Policy: secondPolicy}), http.StatusNotFound)
		})
	}
	response = f.setupRequest(t, "owner", http.MethodGet, second+"/change-review-policy", nil)
	requireNativeStatus(t, response, http.StatusOK)
	var preserved tracker.ChangeReviewPolicy
	decodeHubResponse(t, response, &preserved)
	if preserved.ID != secondPolicy.ID {
		t.Fatal("revoking one project changed the other project's policy")
	}
	response = f.setupRequest(t, "owner", http.MethodGet, second+"/policy", nil)
	requireNativeStatus(t, response, http.StatusOK)
	var current policy.Approval
	decodeHubResponse(t, response, &current)
	secondPolicy.PolicyID = current.Policy.ID
	command := tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "remaining-project"}, ExpectedID: secondPolicy.ID, Policy: secondPolicy}
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, second+"/change-review-policy", command), http.StatusOK)
	command.IdempotencyKey, command.ExpectedID = "other-project-ci", changerequest.PolicyID(secondPolicy)
	command.Policy.RequiredChecks[0].PrincipalID = "ci-" + f.project
	requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, second+"/change-review-policy", command), http.StatusUnprocessableEntity)
}

func TestHostedChangePolicyCIPrincipals(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		role    apiScope
		revoked bool
	}{
		{"independent worker", apiScopeWorker, false},
		{"revoked operator", apiScopeOperator, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			owner := f.user(t, "owner", "owner", "owner@example.test", "write", "")
			requireNativeStatus(t, f.request(t, owner, http.MethodPut, f.base+"/onboarding/policy", policy.Change{Policy: hubTestPolicy()}), http.StatusOK)
			seedHubAPIToken(t, f.service, "ci", "ci-token", test.role)
			if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES ('ci','org_security',?)", f.project); err != nil {
				t.Fatal(err)
			}
			if test.revoked {
				if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE api_tokens SET revoked_at = ? WHERE id = 'ci'", testTimestamp); err != nil {
					t.Fatal(err)
				}
			}
			command := tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "approve"}, Policy: tracker.ChangeReviewPolicy{PolicyID: hubTestPolicy().ID, RequiredChecks: []tracker.ChangeCheckSpec{{Name: "test", PrincipalID: "ci", WorkflowID: "ci.yml", WorkflowSHA256: policy.Digest([]byte("trusted CI")), Source: "independent", MaxAgeSeconds: 3600}}}}
			requireNativeStatus(t, f.request(t, owner, http.MethodPut, f.base+"/change-review-policy", command), http.StatusUnprocessableEntity)
		})
	}
}

func TestHostedChangePolicyBoundaries(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	owner := f.user(t, "owner", "owner", "owner@example.test", "write", "")
	seedHubAPIToken(t, f.service, "admin", "admin-bearer", apiScopeAdmin)
	for _, test := range []struct {
		name, path, csrf, bearer string
		want                     int
	}{
		{"missing csrf", f.base + "/change-review-policy", "", "", http.StatusForbidden},
		{"wrong csrf", f.base + "/change-review-policy", "wrong", "", http.StatusForbidden},
		{"bearer with cookie", f.base + "/change-review-policy", "", "admin-bearer", http.StatusNotFound},
		{"unrelated organization", strings.Replace(f.base, "org_security", "org_unrelated", 1) + "/change-review-policy", hostedCSRF(owner.token), "", http.StatusNotFound},
		{"unknown project", strings.Replace(f.base, string(f.project), "prj_unknown", 1) + "/change-review-policy", hostedCSRF(owner.token), "", http.StatusNotFound},
		{"generic policy administration", f.base + "/policy", hostedCSRF(owner.token), "", http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPut, test.path, strings.NewReader(`{}`))
			request.AddCookie(&http.Cookie{Name: hostedCookie, Value: owner.token})
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("X-CSRF-Token", test.csrf)
			if test.bearer != "" {
				request.Header.Set("Authorization", "Bearer "+test.bearer)
			}
			response := httptest.NewRecorder()
			f.service.Handler().ServeHTTP(response, request)
			requireNativeStatus(t, response, test.want)
		})
	}
	for _, role := range []apiScope{apiScopeWorker, apiScopeOperator, apiScopeAdmin} {
		t.Run("bearer only/"+string(role), func(t *testing.T) {
			seedHubAPIToken(t, f.service, "bearer-"+string(role), string(role)+"-token", role)
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPut, f.base+"/change-review-policy", string(role)+"-token", tracker.ApproveChangeReviewPolicy{}), http.StatusNotFound)
		})
	}
}

type hostedChangePolicyReader struct {
	io.Reader
	beforeRead func()
}

func (r *hostedChangePolicyReader) Read(p []byte) (int, error) {
	if r.beforeRead != nil {
		r.beforeRead()
		r.beforeRead = nil
	}
	return r.Reader.Read(p)
}

func TestHostedChangePolicyRevocation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		revoke func(*testing.T, hostedSecurityFixture, hostedSecurityUser)
	}{
		{"role", func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			t.Helper()
			if err := f.provider.SetMembershipRole(t.Context(), "membership_"+user.identity.Subject, "member"); err != nil {
				t.Fatal(err)
			}
		}},
		{"membership", func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			t.Helper()
			if err := f.provider.RevokeMembership(t.Context(), "membership_"+user.identity.Subject); err != nil {
				t.Fatal(err)
			}
		}},
		{"session", func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			t.Helper()
			if err := f.provider.RevokeSession(t.Context(), user.identity.Hosted.SessionID); err != nil {
				t.Fatal(err)
			}
		}},
		{"write grant", func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			t.Helper()
			f.grant(t, user, false, true)
		}},
		{"local membership", func(t *testing.T, f hostedSecurityFixture, user hostedSecurityUser) {
			t.Helper()
			if err := f.service.revokeHostedMemberLocally(t.Context(), user.identity.Subject); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		for _, replay := range []bool{false, true} {
			t.Run(test.name+"/"+map[bool]string{false: "new", true: "replay"}[replay], func(t *testing.T) {
				t.Parallel()
				f := newHostedSecurityFixture(t)
				user := f.user(t, "owner", "owner", "owner@example.test", "write", "")
				requireNativeStatus(t, f.request(t, user, http.MethodPut, f.base+"/onboarding/policy", policy.Change{Policy: hubTestPolicy()}), http.StatusOK)
				command := tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "approve"}, Policy: tracker.ChangeReviewPolicy{PolicyID: hubTestPolicy().ID, RequireReview: true}}
				if replay {
					requireNativeStatus(t, f.request(t, user, http.MethodPut, f.base+"/change-review-policy", command), http.StatusOK)
				}
				raw, err := json.Marshal(command)
				if err != nil {
					t.Fatal(err)
				}
				body := &hostedChangePolicyReader{Reader: strings.NewReader(string(raw)), beforeRead: func() { test.revoke(t, f, user) }}
				request := httptest.NewRequest(http.MethodPut, f.base+"/change-review-policy", body)
				request.AddCookie(&http.Cookie{Name: hostedCookie, Value: user.token})
				request.Header.Set("X-CSRF-Token", hostedCSRF(user.token))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				f.service.Handler().ServeHTTP(response, request)
				if response.Code != http.StatusForbidden && response.Code != http.StatusNotFound {
					t.Fatalf("revoked approval = %d: %s", response.Code, response.Body.String())
				}
				if body.beforeRead != nil {
					t.Fatal("revocation did not run after middleware authorization")
				}
				var count int
				if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT count(*) FROM change_review_policies").Scan(&count); err != nil {
					t.Fatal(err)
				}
				if count != map[bool]int{false: 0, true: 1}[replay] {
					t.Fatalf("stored policies after revocation = %d", count)
				}
			})
		}
	}
}
