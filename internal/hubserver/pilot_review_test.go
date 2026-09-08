package hubserver

import (
	"net/http"
	"testing"

	"github.com/digitaldrywood/detent/internal/policy"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type pilotReviewVersion struct {
	path     string
	version  tracker.ChangeVersion
	human    bool
	ciStatus int
}

func pilotReviewVersions(t *testing.T, f *browserHostedFixture) []pilotReviewVersion {
	t.Helper()
	organization := f.service.config.Hosted.OrganizationID
	runner := pilotHostedRunner(t, f, 3)
	versions := make([]pilotReviewVersion, 0, 2)
	for _, project := range []string{f.project, f.privateProject} {
		base := "/api/v2/organizations/" + organization + "/projects/" + project
		response := f.page(t, "owner", base+"/policy")
		requireNativeStatus(t, response, http.StatusOK)
		var previous policy.Approval
		decodeHubResponse(t, response, &previous)
		descriptor := previous.Policy
		descriptor.Gates.RequiredChecks, descriptor.Gates.Validator = 1, true
		descriptor = descriptor.WithID()
		requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/onboarding/policy", policy.Change{ExpectedID: previous.Policy.ID, Policy: descriptor}), http.StatusOK)
		principal := "pilot-ci-" + project
		seedHubAPIToken(t, f.service, principal, principal+"-credential", apiScopeOperator)
		if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO token_grants(token_id,organization_id,project_id) VALUES (?,?,?)", principal, organization, project); err != nil {
			t.Fatal(err)
		}
		human := project == f.project
		rules := tracker.ChangeReviewPolicy{PolicyID: descriptor.ID, RequireReview: human, RequiredChecks: []tracker.ChangeCheckSpec{{Name: "test", PrincipalID: principal, WorkflowID: "ci.yml", WorkflowSHA256: policy.Digest([]byte("trusted pilot CI")), Source: "independent", MaxAgeSeconds: 3600}}}
		requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPut, base+"/change-review-policy", tracker.ApproveChangeReviewPolicy{Mutation: tracker.Mutation{IdempotencyKey: "pilot-review-policy"}, Policy: rules}), http.StatusOK)
		response = f.setupRequest(t, "owner", http.MethodPost, base+"/work-items", tracker.CreateIssue{Mutation: tracker.Mutation{IdempotencyKey: "pilot-policy-job"}, Title: "Pilot repository review journey", State: "Todo"})
		requireNativeStatus(t, response, http.StatusOK)
		var issue tracker.NativeIssue
		decodeHubResponse(t, response, &issue)
		path := base + "/work-items/" + string(issue.WorkItemID)
		response = performHubAPIRequest(t, f.service, http.MethodPost, base+"/claims", runner.Credential, tracker.NativeClaim{PolicyID: descriptor.ID, WorkItemID: issue.WorkItemID, MachineID: runner.MachineID, SessionID: "pilot-review-" + project, TTLSeconds: 90, ProtocolMajor: 2, Capabilities: []string{"native_issues", "scoped_collaboration", tracker.NativeExecutionCapability}})
		requireNativeStatus(t, response, http.StatusOK)
		var lease tracker.NativeLease
		decodeHubResponse(t, response, &lease)
		start := nativeStartedEvent(lease)
		finish := start
		finish.Type, finish.IdempotencyKey, finish.Data.Sequence, finish.Data.Outcome = "run.finished", "finish", 2, "succeeded"
		for _, event := range []tracker.NativeRunEvent{start, finish} {
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path+"/events", runner.Credential, event), http.StatusOK)
		}
		requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, base+"/leases/"+string(lease.ID)+"/release", runner.Credential, tracker.NativeLeaseMutation{FencingToken: lease.FencingToken, Reason: "completed"}), http.StatusNoContent)
		response = f.setupRequest(t, "owner", http.MethodPost, path+"/changes", tracker.CreateChange{Mutation: tracker.Mutation{IdempotencyKey: "pilot-review-change"}, Title: "Pilot policy Change Request"})
		requireNativeStatus(t, response, http.StatusOK)
		var change tracker.ChangeRequest
		decodeHubResponse(t, response, &change)
		path += "/changes/" + change.ID
		input := changeTestInput()
		input.PolicyID, input.Repository = descriptor.ID, "https://github.com/example/"+project
		input.External = &tracker.ChangeExternalReference{Provider: "github", ID: "1", URL: input.Repository + "/pull/1"}
		input.RunID, input.AttemptID = start.Data.RunID, start.Data.AttemptID
		response = f.setupRequest(t, "owner", http.MethodPost, path+"/versions", tracker.PublishChangeVersion{Mutation: tracker.Mutation{IdempotencyKey: "pilot-version"}, ChangeVersionInput: input})
		requireNativeStatus(t, response, http.StatusOK)
		var version tracker.ChangeVersion
		decodeHubResponse(t, response, &version)
		if got := pilotChangeSummary(t, f, path); got.Status == "reviewed" || got.Checks != "missing" {
			t.Fatalf("missing CI marked ready: %+v", got)
		}
		response = performHubAPIRequest(t, f.service, http.MethodPost, path+"/versions/"+version.ID+"/checks", principal+"-credential", changeTestResult(version))
		versions = append(versions, pilotReviewVersion{path: path, version: version, human: human, ciStatus: response.Code})
	}
	return versions
}

func pilotChangeSummary(t *testing.T, f *browserHostedFixture, path string) tracker.ChangeSummary {
	t.Helper()
	response := f.page(t, "owner", path)
	requireNativeStatus(t, response, http.StatusOK)
	var detail tracker.ChangeDetail
	decodeHubResponse(t, response, &detail)
	return detail.Summary
}

func TestPilotHostedTwoRepositoryReadiness(t *testing.T) {
	t.Parallel()
	f, _, _ := pilotHostedJourney(t)
	versions := pilotReviewVersions(t, f)
	if len(versions) != 2 {
		t.Fatalf("repository versions = %d, want 2", len(versions))
	}
	for _, version := range versions {
		t.Run(version.version.Repository, func(t *testing.T) {
			summary := pilotChangeSummary(t, f, version.path)
			if version.ciStatus != http.StatusOK || summary.Checks != "success" || summary.ExternalReview != "external_gate" {
				t.Fatalf("independent CI or external gate = %d, %+v", version.ciStatus, summary)
			}
			if !version.human {
				if summary.NativeReview != "not_required" || summary.Status != "reviewed" {
					t.Fatalf("automatic policy review = %+v", summary)
				}
				return
			}
			if summary.NativeReview != "pending" || summary.Status != "needs_evidence" {
				t.Fatalf("human policy bypassed: %+v", summary)
			}
			path := version.path + "/versions/" + version.version.ID + "/reviews"
			review := tracker.ReviewChange{Mutation: tracker.Mutation{IdempotencyKey: "pilot-approval"}, Decision: "approved"}
			requireNativeStatus(t, f.setupRequest(t, "viewer", http.MethodPost, path, review), http.StatusNotFound)
			if got := pilotChangeSummary(t, f, version.path); got.Status == "reviewed" {
				t.Fatal("denied review changed readiness")
			}
			requireNativeStatus(t, f.setupRequest(t, "owner", http.MethodPost, path, review), http.StatusOK)
			if got := pilotChangeSummary(t, f, version.path); got.Status != "reviewed" || got.NativeReview != "approved" || got.Checks != "success" || got.ExternalReview != "external_gate" {
				t.Fatalf("approved repository status = %+v", got)
			}
		})
	}
	if versions[0].version.PolicyID == versions[1].version.PolicyID || versions[0].version.ReviewPolicy.ID == versions[1].version.ReviewPolicy.ID {
		t.Fatal("repository policies share an identity")
	}
	t.Log("PILOT readiness independent_ci_http=200 human_approval_http=200 checks=success human_before_review=needs_evidence human_after_review=reviewed automatic=reviewed external=external_gate repositories=2 shared_runner=true")
}
