package changerequest

import (
	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func ReviewBundleMatches(version tracker.ChangeVersion, ref artifact.Reference) bool {
	if ref.Kind != "diff" || ref.State != "complete" || ref.RunID != version.RunID || ref.AttemptID != version.AttemptID || ref.VersionID != "" && ref.VersionID != version.ID {
		return false
	}
	if ref.SHA256 == version.Code.SHA256 {
		return true
	}
	for _, pinned := range version.Artifacts {
		if (pinned.Kind == "manifest" || pinned.Kind == "diff") && pinned.SHA256 == ref.SHA256 {
			return true
		}
	}
	return false
}
