package hubserver

import (
	"net/http"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/artifact"
)

func TestHostedArtifactAllowanceBoundary(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	ref, publisher, _ := seedHostedArtifact(t, f)
	path := "/api/v2/organizations/org_security/artifact-allowances/" + ref.ServiceID
	request := artifact.Usage{RetainedBytes: 123, ReservedBytes: 456, RelayBytes: 789, StorageRequests: 3, ObservedAt: time.Now().UTC(), WindowStart: time.Now().UTC().Truncate(time.Hour)}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, publisher, request), http.StatusForbidden)
	if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE artifact_services SET binding_json=json_set(binding_json,'$.mode','hosted','$.hosted_opt_in',json('true'))"); err != nil {
		t.Fatal(err)
	}
	response := performHubAPIRequest(t, f.service, http.MethodPost, path, publisher, request)
	requireNativeStatus(t, response, http.StatusOK)
	var limits artifact.Limits
	decodeHubResponse(t, response, &limits)
	if limits.RetainedBytes != 0 || limits.UploadBytes != 0 {
		t.Fatal("free plan granted hosted artifact storage")
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, publisher, request), http.StatusOK)
	usage, err := f.service.database.hostedPlanUsage(t.Context(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if usage.Usage["relay_bytes"] != 789 || usage.Usage["artifact_retained_bytes"] != 123 {
		t.Fatalf("aggregate usage %+v", usage.Usage)
	}
	for _, test := range []struct {
		name, token, path string
		want              int
	}{
		{"operator cannot impersonate publisher", testHubAdminToken, path, http.StatusForbidden},
		{"invalid publisher", "invalid", path, http.StatusForbidden},
		{"unknown service", publisher, "/api/v2/organizations/org_security/artifact-allowances/" + artifact.NewID("service"), http.StatusForbidden},
		{"foreign organization", publisher, "/api/v2/organizations/org_foreign/artifact-allowances/" + ref.ServiceID, http.StatusNotFound},
	} {
		t.Run(test.name, func(t *testing.T) {
			requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, test.path, test.token, request), test.want)
		})
	}
	second := artifact.NewID("service")
	if _, err := f.service.database.db.ExecContext(t.Context(), "INSERT INTO artifact_services(organization_id,project_id,id,binding_json,publisher_token_id) SELECT organization_id,project_id,?,json_set(binding_json,'$.service_id',?),publisher_token_id FROM artifact_services", second, second); err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, "/api/v2/organizations/org_security/artifact-allowances/"+second, publisher, request), http.StatusTooManyRequests)
	if _, err := f.service.database.db.ExecContext(t.Context(), "UPDATE api_tokens SET revoked_at=? WHERE id='artifact-publisher'", formatHubTime(time.Now())); err != nil {
		t.Fatal(err)
	}
	requireNativeStatus(t, performHubAPIRequest(t, f.service, http.MethodPost, path, publisher, request), http.StatusForbidden)
}
