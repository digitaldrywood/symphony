package templates

import (
	"regexp"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/telemetry"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestHostedPagesExcludeInstanceSurfaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mode  string
		title string
	}{
		{mode: "login", title: "Sign in"},
		{mode: "onboarding", title: "Your organization"},
		{mode: "organization", title: "Organization"},
		{mode: "project", title: "Project work"},
		{mode: "denied", title: "Access denied"},
		{mode: "support", title: "Support access"},
		{mode: "unknown", title: "Organization"},
	}
	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			t.Parallel()
			data := hostedTestData(tt.mode)
			html := renderSSEFingerprintComponent(t, HostedPage(data))
			for _, want := range []string{"<title>" + tt.title + " · Detent</title>", `name="viewport"`, `aria-label="Open navigation"`, `md:hidden`, `overflow-y-auto`, `min-h-11`, `name="referrer" content="no-referrer"`} {
				if !strings.Contains(html, want) {
					t.Errorf("hosted page missing %q", want)
				}
			}
			for _, forbidden := range []string{`sse-connect`, `sse-swap`, `hx-get`, `<script`, `/events`, `/fleet`, `/api/v1/`, `/settings`, `/api-keys`, `/analytics`, `/reports`, `/library`, `chat-panel`, `detail-sheet`, `data-work-search`, `data-help`, `data-detent-dashboard-stream`, `data-hosted-impersonation`} {
				if strings.Contains(html, forbidden) {
					t.Errorf("hosted page contains forbidden instance or support surface %q", forbidden)
				}
			}
			if tt.mode != "project" && strings.Contains(html, "private issue") {
				t.Error("non-project page exposed issue content")
			}
			if tt.mode == "login" && (!strings.Contains(html, `href="/auth/oidc/start?unscoped=1"`) || !strings.Contains(html, "Join with invitation")) {
				t.Error("login page has no invitation sign-in path")
			}
			if tt.mode != "organization" && tt.mode != "project" && strings.Contains(html, "Private project") {
				t.Error("page outside organization scope exposed project metadata")
			}
		})
	}
}

func TestHostedAppShellIgnoresInstanceData(t *testing.T) {
	t.Parallel()

	hosted := hostedTestData("organization")
	html := renderSSEFingerprintComponent(t, AppShell(DashboardShellData{
		Hosted: &hosted,
		Title:  "private instance title",
		Projects: []ProjectSmallMultiple{
			{ID: "private-instance-project", Name: "private instance project"},
		},
		Snapshot: telemetry.Snapshot{BoardIssues: []telemetry.Issue{{Title: "private instance issue"}}},
	}, nil))
	if strings.Contains(html, "private instance") || strings.Contains(html, "private-instance") {
		t.Error("hosted shell exposed instance data")
	}
	if !strings.Contains(html, "Private project") {
		t.Error("hosted shell lost explicitly supplied hosted project navigation")
	}
}

func TestHostedFormsCarryCSRF(t *testing.T) {
	t.Parallel()

	forms := regexp.MustCompile(`(?s)<form\b([^>]*)>(.*?)</form>`)
	for _, mode := range []string{"onboarding", "organization", "project", "support", "denied"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			data := hostedTestData(mode)
			data.CanManage, data.CanCreate, data.CanManageRunners, data.CanManageOwnership, data.CanSupport = true, true, true, true, true
			html := renderSSEFingerprintComponent(t, HostedPage(data))
			matches := forms.FindAllStringSubmatch(html, -1)
			if len(matches) == 0 {
				t.Fatal("expected authenticated forms")
			}
			for _, match := range matches {
				if !strings.Contains(match[1], `method="post"`) || !strings.Contains(match[2], `type="hidden" name="csrf" value="csrf-example"`) {
					t.Errorf("form has no protected POST: %s", match[1])
				}
			}
			if mode == "organization" {
				for _, want := range []string{`action="/organization/switch"`, `action="/organization/invite"`, `action="/organization/members/member-example/role"`, `action="/organization/members/member-example/revoke"`, `action="/organization/grants"`, `action="/projects"`, `name="grant_access" type="checkbox" value="true" required`, `name="write" value="true"`, `name="runner" value="true"`, `name="revoke" value="true"`, `maxlength="120"`} {
					if !strings.Contains(html, want) {
						t.Errorf("organization form missing %q", want)
					}
				}
				if strings.Contains(html, `action="/organization/create"`) || strings.Contains(html, ` checked`) {
					t.Error("organization page exposes organization creation or preselects a grant")
				}
			}
			if mode == "onboarding" {
				for _, want := range []string{`action="/organization/create"`, `action="/organization/join"`, `name="token" type="password"`} {
					if !strings.Contains(html, want) {
						t.Errorf("onboarding form missing %q", want)
					}
				}
			}
		})
	}
}

func TestHostedPermissionControls(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		manage    bool
		create    bool
		runners   bool
		ownership bool
	}{
		{name: "viewer"},
		{name: "creator", create: true},
		{name: "admin", manage: true},
		{name: "runner administrator", manage: true, runners: true},
		{name: "owner", manage: true, ownership: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := hostedTestData("organization")
			data.CanManage, data.CanCreate, data.CanManageRunners, data.CanManageOwnership = tt.manage, tt.create, tt.runners, tt.ownership
			data.Members = append(data.Members, HostedMember{ID: "owner-example", UserID: "owner-user", Role: "owner"})
			html := renderSSEFingerprintComponent(t, HostedPage(data))
			for _, control := range []struct {
				value string
				want  bool
			}{
				{value: `action="/organization/invite"`, want: tt.manage},
				{value: `action="/projects"`, want: tt.create},
				{value: `name="runner"`, want: tt.manage},
				{value: `value="owner"`, want: tt.ownership},
				{value: `action="/organization/members/owner-example/revoke"`, want: tt.ownership},
			} {
				if got := strings.Contains(html, control.value); got != control.want {
					t.Errorf("control %q visible = %t, want %t", control.value, got, control.want)
				}
			}
		})
	}
}

func TestHostedSelectionsPreserveRoleAndOrganization(t *testing.T) {
	t.Parallel()

	selects := regexp.MustCompile(`(?s)<select\b([^>]*)>(.*?)</select>`)
	options := regexp.MustCompile(`<option\b([^>]*)>`)
	values := regexp.MustCompile(`\bvalue="([^"]*)"`)
	selected := regexp.MustCompile(`(?:^|\s)selected(?:\s|=|$)`)
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		t.Run(role, func(t *testing.T) {
			t.Parallel()
			data := hostedTestData("organization")
			data.CanManage, data.CanManageOwnership = true, true
			data.Members[0].Role = role
			data.Organizations = []HostedOrganizationChoice{
				{ID: "org-first", Name: "First organization"},
				{ID: data.OrganizationID, Name: "Current organization"},
				{ID: "org-last", Name: "Last organization"},
			}
			html := renderSSEFingerprintComponent(t, HostedPage(data))
			checked := 0
			for _, control := range selects.FindAllStringSubmatch(html, -1) {
				var want string
				switch {
				case strings.Contains(control[1], `id="hosted-invite-role"`):
					want = "member"
				case strings.Contains(control[1], `name="role"`):
					want = role
				case strings.Contains(control[1], `name="organization"`):
					want = data.OrganizationID
				default:
					continue
				}
				checked++
				selectedCount := 0
				for _, option := range options.FindAllStringSubmatch(control[2], -1) {
					if !selected.MatchString(option[1]) {
						continue
					}
					selectedCount++
					value := values.FindStringSubmatch(option[1])
					if len(value) != 2 || value[1] != want {
						t.Errorf("control %s selects option %s, want %q", control[1], option[1], want)
					}
					if strings.Contains(option[1], "selected=") {
						t.Errorf("option %s must emit selected as a boolean attribute", option[1])
					}
				}
				if selectedCount != 1 {
					t.Errorf("control %s has %d selected attributes, want exactly one", control[1], selectedCount)
				}
			}
			if checked != 3 {
				t.Errorf("checked %d selection controls, want invitation, membership and organization", checked)
			}
		})
	}
}

func TestHostedSupportIdentityIndicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		authorized  bool
		impersonate bool
	}{
		{name: "ordinary staff"},
		{name: "authorized support", authorized: true},
		{name: "active impersonation", authorized: true, impersonate: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			data := hostedTestData("support")
			data.CanSupport = tt.authorized
			if tt.impersonate {
				data.SupportActor, data.SupportReason, data.SupportExpiry = "support@example.com", "Customer requested help", "2026-09-07T12:00:00Z"
			}
			html := renderSSEFingerprintComponent(t, HostedPage(data))
			if got := strings.Contains(html, `action="/support/start"`); got != (tt.authorized && !tt.impersonate) {
				t.Errorf("support start visible = %t", got)
			}
			if got := strings.Contains(html, `data-hosted-impersonation`); got != tt.impersonate {
				t.Errorf("support indicator visible = %t", got)
			}
			if tt.impersonate {
				for _, want := range []string{data.SupportActor, data.Email, data.OrganizationName, data.SupportReason, data.SupportExpiry, "Exit support session", `action="/logout"`} {
					if !strings.Contains(html, want) {
						t.Errorf("support indicator missing %q", want)
					}
				}
			}
		})
	}
}

func TestHostedProjectContentAndEscaping(t *testing.T) {
	t.Parallel()

	data := hostedTestData("project")
	data.Title = "<script>title</script>"
	data.Error, data.Notice = "<script>error</script>", "<script>notice</script>"
	data.Issues = append(data.Issues,
		tracker.NativeIssue{NativeReference: tracker.NativeReference{OrganizationID: "other-org", ProjectID: "project-example"}, Title: "other organization content"},
		tracker.NativeIssue{NativeReference: tracker.NativeReference{OrganizationID: "org-example", ProjectID: "other-project"}, Title: "other project content"},
	)
	html := renderSSEFingerprintComponent(t, HostedPage(data))
	for _, forbidden := range []string{"other organization content", "other project content", "private issue body", "<script>"} {
		if strings.Contains(html, forbidden) {
			t.Errorf("project page contains %q", forbidden)
		}
	}
	for _, want := range []string{`href="/api/v2/organizations/org-example/projects/project-example/work-items/issue-example"`, "private issue", "&lt;script&gt;title&lt;/script&gt;", `role="alert"`, `role="status"`} {
		if !strings.Contains(html, want) {
			t.Errorf("project page missing %q", want)
		}
	}
}

func hostedTestData(mode string) HostedPageData {
	return HostedPageData{
		Mode: mode, Email: "customer@example.com", OrganizationID: "org-example", OrganizationName: "Example organization", SelectedProject: "project-example", CSRF: "csrf-example",
		Organizations: []HostedOrganizationChoice{{ID: "org-example", Name: "Example organization"}},
		Projects:      []HostedProjectChoice{{ID: "project-example", Name: "Private project"}},
		Members:       []HostedMember{{ID: "member-example", UserID: "user-example", Role: "member"}},
		Issues: []tracker.NativeIssue{{
			NativeReference: tracker.NativeReference{OrganizationID: "org-example", ProjectID: "project-example", WorkItemID: "issue-example"},
			Title:           "private issue", Body: "private issue body", State: "Todo",
		}},
	}
}
