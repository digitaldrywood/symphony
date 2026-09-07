package changerequest

import (
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/artifact"
	"github.com/digitaldrywood/detent/internal/tracker"
)

func TestReviewBundleMatches(t *testing.T) {
	t.Parallel()
	version := tracker.ChangeVersion{ID: "version", ChangeVersionInput: tracker.ChangeVersionInput{RunID: "run", AttemptID: "attempt", Code: tracker.ChangeArtifact{SHA256: strings.Repeat("a", 64)}, Artifacts: []tracker.ChangeArtifact{{Kind: "manifest", SHA256: strings.Repeat("b", 64)}}}}
	for _, tt := range []struct {
		name string
		edit func(*artifact.Reference)
		want bool
	}{
		{"pinned code", func(*artifact.Reference) {}, true},
		{"pinned manifest", func(r *artifact.Reference) { r.SHA256 = strings.Repeat("b", 64) }, true},
		{"unversioned matching attempt", func(r *artifact.Reference) { r.VersionID = "" }, true},
		{"other version", func(r *artifact.Reference) { r.VersionID = "other" }, false},
		{"other run", func(r *artifact.Reference) { r.RunID = "other" }, false},
		{"other attempt", func(r *artifact.Reference) { r.AttemptID = "other" }, false},
		{"unbound digest", func(r *artifact.Reference) { r.SHA256 = strings.Repeat("c", 64) }, false},
		{"partial", func(r *artifact.Reference) { r.State = "partial" }, false},
		{"log", func(r *artifact.Reference) { r.Kind = "log" }, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ref := artifact.Reference{Scope: artifact.Scope{RunID: "run", AttemptID: "attempt", VersionID: "version"}, Kind: "diff", State: "complete", SHA256: version.Code.SHA256}
			tt.edit(&ref)
			if got := ReviewBundleMatches(version, ref); got != tt.want {
				t.Fatalf("matches = %v, want %v", got, tt.want)
			}
		})
	}
}
