package hubserver

import (
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"

	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestHostedWorkAccess(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, role, grant, email, revoke string
		allowed, write                   bool
	}{
		{name: "owner", role: "owner", grant: "write", allowed: true, write: true},
		{name: "member", role: "member", grant: "write", allowed: true, write: true},
		{name: "viewer", role: "viewer", grant: "write", allowed: true},
		{name: "read grant", role: "member", grant: "read", allowed: true},
		{name: "no grant", role: "owner"},
		{name: "staff", role: "owner", grant: "write", email: "staff@example.test"},
		{name: "revoked session", role: "owner", grant: "write", revoke: "session"},
		{name: "revoked membership", role: "owner", grant: "write", revoke: "member"},
		{name: "revoked grant", role: "owner", grant: "write", revoke: "grant"},
		{name: "wrong tenant", role: "owner", grant: "write", revoke: "tenant"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			f := newHostedSecurityFixture(t)
			owner := f.user(t, "owner", "owner", "owner@example.test", "write", "")
			item := f.seedIssue(t, 1)
			api := f.base + "/work-items/" + string(item)
			response := f.request(t, owner, http.MethodPost, api+"/changes", tracker.CreateChange{Mutation: tracker.Mutation{IdempotencyKey: "change"}, Title: "private-change-sentinel", Body: "private-change-body"})
			requireNativeStatus(t, response, http.StatusOK)
			var change tracker.ChangeRequest
			decodeHubResponse(t, response, &change)
			email := test.email
			if email == "" {
				email = "member@example.test"
			}
			user := f.user(t, "member", test.role, email, test.grant, "")
			switch test.revoke {
			case "session":
				if err := f.provider.RevokeSession(t.Context(), user.identity.Hosted.SessionID); err != nil {
					t.Fatal(err)
				}
			case "member":
				if err := f.provider.RevokeMembership(t.Context(), "membership_"+user.identity.Subject); err != nil {
					t.Fatal(err)
				}
			case "grant":
				if _, err := f.service.database.db.ExecContext(t.Context(), "DELETE FROM hosted_project_grants WHERE user_id = ?", user.identity.Subject); err != nil {
					t.Fatal(err)
				}
			case "tenant":
				f.provider.mu.Lock()
				identity := f.provider.sessions[user.identity.Hosted.SessionID]
				identity.OrganizationID = "org_other"
				f.provider.sessions[user.identity.Hosted.SessionID] = identity
				f.provider.mu.Unlock()
			}
			page := "/projects/" + string(f.project) + "/issues/" + string(item)
			for _, path := range []string{page, page + "/changes/" + change.ID, "/projects/" + string(f.project) + "/changes"} {
				response := f.request(t, user, http.MethodGet, path, nil)
				if test.allowed {
					requireNativeStatus(t, response, http.StatusOK)
					if !strings.Contains(response.Body.String(), "private-") {
						t.Error("missing authorized content")
					}
					hasForm := strings.Contains(response.Body.String(), "data-work-action")
					if !strings.HasSuffix(path, "/changes") && hasForm != test.write {
						t.Errorf("write form = %t, want %t", hasForm, test.write)
					}
				} else {
					status := http.StatusForbidden
					if test.revoke == "session" || test.revoke == "tenant" {
						status = http.StatusUnauthorized
					}
					requireNativeStatus(t, response, status)
					if strings.Contains(response.Body.String(), "private-") {
						t.Error("denial exposed private content")
					}
				}
				if !strings.Contains(response.Header().Get("Content-Type"), "text/html") || response.Header().Get("Cache-Control") != "no-store" {
					t.Error("hosted work must return uncached HTML")
				}
			}
			for _, suffix := range []string{"/comments", "/history", "/attempts", "/changes", "/artifacts"} {
				response := f.request(t, user, http.MethodGet, api+suffix, nil)
				if test.allowed {
					requireNativeStatus(t, response, http.StatusOK)
				} else if response.Code == http.StatusOK {
					t.Errorf("revoked API read allowed: %s", suffix)
				}
			}
			response = f.request(t, user, http.MethodPost, api+"/comments", tracker.CreateComment{Mutation: tracker.Mutation{IdempotencyKey: "comment"}, Body: "discussion"})
			if test.write {
				requireNativeStatus(t, response, http.StatusOK)
			} else if response.Code == http.StatusOK {
				t.Error("read-only comment allowed")
			}
		})
	}
}

func TestHostedWorkMissingAndForeignResources(t *testing.T) {
	t.Parallel()
	f := newBrowserHostedFixture(t, true)
	var item string
	if err := f.service.database.db.QueryRowContext(t.Context(), "SELECT native_id FROM issues WHERE project_id = ?", f.project).Scan(&item); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/projects/" + f.project + "/issues/missing",
		"/projects/" + f.privateProject + "/issues/" + item,
		"/projects/" + f.project + "/issues/" + item + "/changes/missing",
	} {
		t.Run(path, func(t *testing.T) {
			response := f.page(t, "owner", path)
			requireNativeStatus(t, response, http.StatusNotFound)
			if strings.Contains(response.Body.String(), "Browser fixture private body") {
				t.Error("foreign item leaked")
			}
		})
	}
}

func TestHostedWorkErrors(t *testing.T) {
	t.Parallel()
	f := newHostedSecurityFixture(t)
	for _, test := range []struct {
		name   string
		err    error
		status int
	}{
		{"missing row", sql.ErrNoRows, http.StatusNotFound},
		{"missing native", nativeNotFound(), http.StatusNotFound},
		{"storage failure", errors.New("private database details"), http.StatusServiceUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			c := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), response)
			if err := f.service.hostedWorkError(c, test.err); err != nil {
				t.Fatal(err)
			}
			requireNativeStatus(t, response, test.status)
			if strings.Contains(response.Body.String(), "private database") {
				t.Error("internal error disclosed")
			}
		})
	}
}
