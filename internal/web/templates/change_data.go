package templates

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/changerequest"
	"github.com/digitaldrywood/detent/internal/tracker"
)

type ChangePageData struct {
	Artifacts         []artifact.Reference
	ArtifactError     string
	ArtifactFormToken string
	Dashboard         DashboardData
	ProjectID         string
	IssueID           tracker.NativeWorkItemID
	ChangeID          string
	VersionID         string
	Detail            *tracker.ChangeDetail
	Loading           bool
	Error             string
}

func (data ChangePageData) ReviewBundles() []artifact.Reference {
	version, ok := data.Version()
	if !ok {
		return nil
	}
	var refs []artifact.Reference
	for _, ref := range data.Artifacts {
		if changerequest.ReviewBundleMatches(version, ref) {
			refs = append(refs, ref)
		}
	}
	return refs
}

func (data ChangePageData) ReviewConfig() string {
	version, ok := data.Version()
	if !ok || data.Detail == nil {
		return "{}"
	}
	encoded, err := json.Marshal(struct {
		Version tracker.ChangeVersion `json:"version"`
		Change  tracker.ChangeRequest `json:"change"`
		Bundles []artifact.Reference  `json:"bundles"`
	}{version, data.Detail.Change, data.ReviewBundles()})
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func NativeIssuePath(projectID string, item tracker.NativeWorkItemID) string {
	return "/projects/" + url.PathEscape(projectID) + "/issues/" + url.PathEscape(string(item))
}

func ChangePath(projectID string, item tracker.NativeWorkItemID, id string) string {
	return NativeIssuePath(projectID, item) + "/changes/" + url.PathEscape(id)
}

func (data ChangePageData) Path() string {
	path := ChangePath(data.ProjectID, data.IssueID, data.ChangeID)
	if data.VersionID != "" {
		path += "?version=" + url.QueryEscape(data.VersionID)
	}
	return path
}

func (data ChangePageData) ContentPath() string {
	path := data.Path()
	if data.VersionID != "" {
		return path + "&content=1"
	}
	return path + "?content=1"
}

func (data ChangePageData) Version() (tracker.ChangeVersion, bool) {
	if data.Detail == nil {
		return tracker.ChangeVersion{}, false
	}
	id := data.VersionID
	if id == "" {
		id = data.Detail.Change.CurrentVersion
	}
	for _, version := range data.Detail.Versions {
		if version.ID == id {
			return version, true
		}
	}
	return tracker.ChangeVersion{}, false
}

func changeShellData(data ChangePageData) DashboardShellData {
	shell := DashboardShellDataFromDashboard(data.Dashboard)
	shell.Title = "Change Request - Detent"
	shell.ActiveNav = "project"
	return shell
}

func changeVersionPath(data ChangePageData, version tracker.ChangeVersion) string {
	return ChangePath(data.ProjectID, data.IssueID, data.ChangeID) + "?version=" + url.QueryEscape(version.ID)
}

func nativeRunPath(projectID string, item tracker.NativeWorkItemID, attempt string) string {
	return NativeIssuePath(projectID, item) + "/runs/" + url.PathEscape(attempt)
}

func changeStatusLabel(value string) string {
	return strings.ReplaceAll(value, "_", " ")
}

func changeVersionCurrent(data ChangePageData, id string) string {
	version, ok := data.Version()
	if ok && version.ID == id {
		return "page"
	}
	return "false"
}
