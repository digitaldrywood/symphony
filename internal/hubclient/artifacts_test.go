package hubclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/digitaldrywood/detent/internal/artifact"
)

func TestArtifactJournalReplay(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"lost reply", "restart after short chunk", "unicode boundary", "disconnected"} {
		t.Run(scenario, func(t *testing.T) {
			directory := t.TempDir()
			input := strings.Repeat("x", 64<<10) + "tail"
			if scenario == "unicode boundary" {
				input = strings.Repeat("x", (64<<10)-1) + "🌲tail"
			}
			if scenario == "restart after short chunk" {
				input = "short"
			}
			var parts []artifact.Part
			lose, down := scenario == "lost reply", scenario == "disconnected"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if down {
					w.WriteHeader(503)
					_, _ = w.Write([]byte(`{"code":"storage_unreachable"}`))
					return
				}
				if r.URL.Path == "/v1/uploads" {
					_ = json.NewEncoder(w).Encode(artifact.Upload{ArtifactID: "artifact_" + strings.Repeat("a", 32), State: "uploading"})
					return
				}
				var p artifact.Part
				if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
					t.Error(err)
					w.WriteHeader(422)
					return
				}
				if p.Sequence < len(parts) {
					if !bytes.Equal(parts[p.Sequence].Data, p.Data) {
						t.Error("retry changed chunk")
						w.WriteHeader(409)
						return
					}
				} else if p.Sequence == len(parts) {
					parts = append(parts, p)
				} else {
					t.Error("chunk gap")
				}
				if lose {
					lose = false
					w.WriteHeader(503)
					_, _ = w.Write([]byte(`{"code":"storage_unreachable"}`))
					return
				}
				_ = json.NewEncoder(w).Encode(artifact.Object{})
			}))
			defer server.Close()
			newExecution := func() *nativeExecution {
				return &nativeExecution{artifacts: nativeArtifacts{directory: directory, client: &artifact.Client{Origin: server.URL, Token: func(_ context.Context) (string, error) { return "worker", nil }}, reservation: artifact.Reservation{Bytes: 4 << 20}}}
			}
			e := newExecution()
			err := e.ArtifactLog(t.Context(), input)
			if scenario == "lost reply" || scenario == "disconnected" {
				if !errors.Is(err, artifact.ErrStorage) {
					t.Fatal(err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if scenario == "restart after short chunk" {
				if err := e.artifacts.flush(t.Context(), true); err != nil {
					t.Fatal(err)
				}
			}
			down = false
			e = newExecution()
			if scenario == "restart after short chunk" {
				if err := e.ArtifactLog(t.Context(), " after restart"); err != nil {
					t.Fatal(err)
				}
				input += " after restart"
			}
			if err := e.artifacts.flush(t.Context(), true); err != nil {
				t.Fatal(err)
			}
			var got []byte
			for index, p := range parts {
				if p.Sequence != index || p.SHA256 != artifact.Digest(p.Data) || !utf8.Valid(p.Data) {
					t.Fatal("invalid part", index)
				}
				frozen, err := os.ReadFile(filepath.Join(directory, "part-"+strconv.Itoa(index)))
				if err != nil || !bytes.Equal(frozen, p.Data) {
					t.Fatal("chunk journal", err)
				}
				got = append(got, p.Data...)
			}
			if string(got) != input {
				t.Fatal("journal replay lost or duplicated bytes")
			}
		})
	}
}

func TestArtifactJournalRejectsInvalidAndOversizeLogs(t *testing.T) {
	t.Parallel()
	for _, delta := range []string{string([]byte{255}), strings.Repeat("x", 1025)} {
		t.Run(strconv.Itoa(len(delta)), func(t *testing.T) {
			e := &nativeExecution{artifacts: nativeArtifacts{directory: t.TempDir(), reservation: artifact.Reservation{Bytes: artifact.MaxManifestBytes + 1024}}}
			if err := e.ArtifactLog(t.Context(), delta); err == nil || !e.artifacts.incomplete {
				t.Fatal("invalid log accepted", err)
			}
		})
	}
}
