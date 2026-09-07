package artifact

import (
	"bytes"
	"fmt"
	"image"
	"image/png"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/digitaldrywood/detent/internal/testenv"
)

func TestMain(m *testing.M) {
	if err := testenv.ClearGitEnvironment(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func captureGitCommand(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(t.Context(), "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=Artifact Test", "GIT_AUTHOR_EMAIL=artifact@example.invalid", "GIT_COMMITTER_NAME=Artifact Test", "GIT_COMMITTER_EMAIL=artifact@example.invalid", "GIT_CONFIG_NOSYSTEM=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func TestCaptureGitChangedFileContext(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name        string
		contents    []byte
		wantParts   int
		unsupported bool
	}{
		{"text", []byte("complete head context\nnew line\n"), 5, false},
		{"binary", []byte{0, 1, 2}, 4, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			captureGitCommand(t, dir, "init", "-b", "main")
			for _, name := range []string{"changed.txt", "deleted.txt", "unchanged.txt"} {
				if err := os.WriteFile(filepath.Join(dir, name), []byte("complete base context\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			captureGitCommand(t, dir, "add", ".")
			captureGitCommand(t, dir, "-c", "commit.gpgsign=false", "commit", "-m", "base")
			base := captureGitCommand(t, dir, "rev-parse", "HEAD")
			if err := os.WriteFile(filepath.Join(dir, "changed.txt"), test.contents, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "added.txt"), []byte("added file\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			captureGitCommand(t, dir, "rm", "deleted.txt")
			captureGitCommand(t, dir, "add", ".")
			captureGitCommand(t, dir, "-c", "commit.gpgsign=false", "commit", "-m", "head")
			head := captureGitCommand(t, dir, "rev-parse", "HEAD")
			bundle, err := CaptureGit(t.Context(), dir, base, head, 3)
			if test.unsupported {
				if err == nil {
					t.Fatal("binary accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if bundle.Capture.Base != base || bundle.Capture.Head != head || bundle.Capture.MergeBase != base || bundle.Capture.WorkingTree || len(bundle.Parts) != test.wantParts {
				t.Fatalf("capture: %#v", bundle.Capture)
			}
			if test.name == "binary" && !bytes.Contains(bundle.Parts[0].Data, []byte("Binary files")) {
				t.Fatal("binary change disappeared from the patch")
			}
			for _, p := range bundle.Parts {
				if p.SHA256 != Digest(p.Data) || p.Path == "unchanged.txt" {
					t.Fatal("bad context", p.Path)
				}
				if p.Path == "changed.txt" && p.Side == "head" && !bytes.Equal(p.Data, test.contents) {
					t.Fatal("head truncated")
				}
				if bytes.ContainsRune(p.Data, 0) {
					t.Fatal("binary body entered text bundle")
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "changed.txt"), []byte("uncommitted"), 0o600); err != nil {
				t.Fatal(err)
			}
			again, err := CaptureGit(t.Context(), dir, base, head, 3)
			if err != nil || !bytes.Equal(again.Parts[0].Data, bundle.Parts[0].Data) {
				t.Fatal("working tree changed immutable capture", err)
			}
			for _, refs := range [][2]string{{"--bad", head}, {base, "missing"}, {"", head}} {
				if _, err := CaptureGit(t.Context(), dir, refs[0], refs[1], 3); err == nil {
					t.Fatal("bad refs accepted")
				}
			}
		})
	}
}

func TestCaptureGitRename(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	captureGitCommand(t, dir, "init", "-b", "main")
	if err := os.WriteFile(filepath.Join(dir, "old.go"), []byte("package example\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	captureGitCommand(t, dir, "add", ".")
	captureGitCommand(t, dir, "-c", "commit.gpgsign=false", "commit", "-m", "base")
	base := captureGitCommand(t, dir, "rev-parse", "HEAD")
	captureGitCommand(t, dir, "mv", "old.go", "new.go")
	captureGitCommand(t, dir, "-c", "commit.gpgsign=false", "commit", "-m", "rename")
	bundle, err := CaptureGit(t.Context(), dir, base, "HEAD", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.Parts) != 3 || !bytes.Contains(bundle.Parts[0].Data, []byte("rename from old.go\nrename to new.go")) {
		t.Fatal("rename identity or changed-file context lost")
	}
}

func TestMediaValidation(t *testing.T) {
	t.Parallel()
	var picture bytes.Buffer
	if err := png.Encode(&picture, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, media string
		data        []byte
		valid       bool
	}{
		{"png", "image/png", picture.Bytes(), true}, {"wrong image", "image/jpeg", picture.Bytes(), false},
		{"text", "text/plain; charset=utf-8", []byte("<script>display as text</script>"), true},
		{"invalid utf8", "text/plain; charset=utf-8", []byte{255}, false},
		{"mp4", "video/mp4", []byte{0, 0, 0, 12, 'f', 't', 'y', 'p', 0, 0, 0, 0}, true},
		{"webm", "video/webm", []byte{0x1a, 0x45, 0xdf, 0xa3}, true},
		{"invalid video", "video/mp4", []byte("bad"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if valid := validateMedia(test.media, test.data) == nil; valid != test.valid {
				t.Fatal(valid)
			}
		})
	}
}

func TestCaptureGitLiteralPaths(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"a[1].txt", ":(exclude)secret.txt"} {
		t.Run(name, func(t *testing.T) {
			if runtime.GOOS == "windows" && strings.Contains(name, ":") {
				t.Skip("Windows filenames cannot contain colons")
			}
			dir := t.TempDir()
			captureGitCommand(t, dir, "init", "-b", "main")
			captureGitCommand(t, dir, "-c", "commit.gpgsign=false", "commit", "--allow-empty", "-m", "base")
			base := captureGitCommand(t, dir, "rev-parse", "HEAD")
			if err := os.WriteFile(filepath.Join(dir, name), []byte("literal context\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			captureGitCommand(t, dir, "add", ".")
			captureGitCommand(t, dir, "-c", "commit.gpgsign=false", "commit", "-m", "head")
			bundle, err := CaptureGit(t.Context(), dir, base, "HEAD", 3)
			if err != nil {
				t.Fatal(err)
			}
			if len(bundle.Parts) != 2 || bundle.Parts[1].Path != name || string(bundle.Parts[1].Data) != "literal context\n" {
				t.Fatal("literal file context was lost")
			}
		})
	}
}
