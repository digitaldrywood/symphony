package templates

import (
	"net/url"

	"github.com/digitaldrywood/detent/internal/onboarding"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type HostedPageData struct {
	Setup               *onboarding.Project
	SetupAPI            string
	CanWriteProject     bool
	ProjectStates       []tracker.NativeState
	IntegrationSummary  string
	IntegrationRevision string
	GitHubRepository    string
	GitHubIntake        string
	GitHubProjection    string
	GitHubPR            bool
	GitHubAvailable     bool
	Assets              AssetPaths
	Title               string
	Email               string
	OrganizationName    string
	OrganizationID      string
	CSRF                string
	Error               string
	Notice              string
	Mode                string
	SelectedProject     string
	CanManage           bool
	CanCreate           bool
	CanManageRunners    bool
	CanManageOwnership  bool
	CanSupport          bool
	SupportActor        string
	SupportReason       string
	SupportExpiry       string
	Organizations       []HostedOrganizationChoice
	Projects            []HostedProjectChoice
	Members             []HostedMember
	Issues              []tracker.NativeIssue
}

type HostedOrganizationChoice struct {
	ID   string
	Name string
}

type HostedProjectChoice struct {
	ID   string
	Name string
}

type HostedMember struct {
	ID     string
	UserID string
	Role   string
}

func hostedPageTitle(data HostedPageData) string {
	if data.Title != "" {
		return data.Title
	}
	switch data.Mode {
	case "login":
		return "Sign in"
	case "onboarding":
		return "Your organization"
	case "project":
		return "Project work"
	case "denied":
		return "Access denied"
	case "support":
		return "Support access"
	default:
		return "Organization"
	}
}

func hostedProjectPath(project string) string {
	return "/projects/" + url.PathEscape(project)
}

func hostedMemberPath(member string, action string) string {
	return "/organization/members/" + url.PathEscape(member) + "/" + action
}

func hostedIssuePath(organization string, project string, issue tracker.NativeWorkItemID) string {
	return "/api/v2/organizations/" + url.PathEscape(organization) + "/projects/" + url.PathEscape(project) + "/work-items/" + url.PathEscape(string(issue))
}

func hostedProjectMode(data HostedPageData) bool {
	return data.OrganizationID != "" && (data.Mode == "organization" || data.Mode == "project")
}
