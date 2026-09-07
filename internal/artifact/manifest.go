package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

const (
	MaxManifestBytes = 1 << 20
	MaxChunkBytes    = 1 << 20
	MaxTextBytes     = 16 << 20
	MaxVideoBytes    = 64 << 20
	MaxArtifactBytes = 256 << 20
	MaxObjects       = 1024
)

var (
	ErrInvalid       = errors.New("invalid_manifest")
	ErrIntegrity     = errors.New("checksum_mismatch")
	ErrMissing       = errors.New("missing")
	ErrStorage       = errors.New("storage_unreachable")
	ErrUnsupported   = errors.New("unsupported_capability")
	ErrConflict      = errors.New("conflict")
	ErrQuota         = errors.New("quota_exceeded")
	ErrExpired       = errors.New("expired")
	ErrDenied        = errors.New("denied")
	ErrAuthorization = errors.New("authorization_unavailable")
)

type Scope struct {
	OrganizationID string `json:"organization_id"`
	ProjectID      string `json:"project_id"`
	WorkItemID     string `json:"work_item_id"`
	RunID          string `json:"run_id,omitempty"`
	AttemptID      string `json:"attempt_id,omitempty"`
	VersionID      string `json:"version_id,omitempty"`
}

type Object struct {
	ID        string `json:"object_id"`
	MediaType string `json:"media_type"`
	Size      int64  `json:"size"`
	SHA256    string `json:"sha256"`
	Path      string `json:"path,omitempty"`
	Side      string `json:"side,omitempty"`
	Sequence  int    `json:"sequence"`
	Offset    int64  `json:"offset"`
}

type Capture struct {
	Base         string `json:"base"`
	Head         string `json:"head"`
	MergeBase    string `json:"merge_base"`
	ContextLines int    `json:"context_lines"`
	FileContext  string `json:"file_context"`
	WorkingTree  bool   `json:"working_tree"`
}

type Manifest struct {
	SchemaVersion int `json:"schema_version"`
	Scope
	ArtifactID        string    `json:"artifact_id"`
	ManifestID        string    `json:"manifest_id"`
	Revision          int64     `json:"revision"`
	Kind              string    `json:"kind"`
	State             string    `json:"state"`
	CreatedAt         time.Time `json:"created_at"`
	ExpiresAt         time.Time `json:"expires_at"`
	RetentionPolicyID string    `json:"retention_policy_id"`
	TotalBytes        int64     `json:"total_bytes"`
	Objects           []Object  `json:"objects"`
	Capture           *Capture  `json:"capture,omitempty"`
}

type Reference struct {
	SchemaVersion int `json:"schema_version"`
	Scope
	ServiceID    string    `json:"service_id"`
	ArtifactID   string    `json:"artifact_id"`
	ManifestID   string    `json:"manifest_id"`
	Revision     int64     `json:"revision"`
	SHA256       string    `json:"sha256"`
	Kind         string    `json:"kind"`
	State        string    `json:"state"`
	Availability string    `json:"availability"`
	Bytes        int64     `json:"bytes"`
	Objects      int       `json:"objects"`
	ExpiresAt    time.Time `json:"expires_at"`
	ObservedAt   time.Time `json:"observed_at"`
}

func NewID(prefix string) string { return prefix + "_" + strings.ReplaceAll(uuid.NewString(), "-", "") }

func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ValidID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix+"_") {
		return false
	}
	return ValidHash(strings.TrimPrefix(value, prefix+"_"), 32)
}

func ValidHash(value string, length int) bool {
	if len(value) != length || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func (s Scope) Validate() error {
	for _, field := range []struct{ value, prefix string }{{s.OrganizationID, "org"}, {s.ProjectID, "prj"}, {s.WorkItemID, "wi"}} {
		if !ValidID(field.value, field.prefix) {
			return ErrInvalid
		}
	}
	if !ValidID(s.AttemptID, "attempt") || !ValidID(s.RunID, "run") {
		return ErrInvalid
	}
	if s.VersionID != "" && !ValidID(s.VersionID, "version") {
		return ErrInvalid
	}
	return nil
}

func (m Manifest) Validate() error {
	if m.Scope.Validate() != nil || m.SchemaVersion != 1 || !ValidID(m.ArtifactID, "artifact") || !ValidID(m.ManifestID, "manifest") || m.Revision < 1 || !slices.Contains([]string{"log", "diff", "screenshot", "video"}, m.Kind) || !slices.Contains([]string{"partial", "complete", "interrupted"}, m.State) || m.CreatedAt.IsZero() || !m.ExpiresAt.After(m.CreatedAt) || m.RetentionPolicyID == "" || len(m.RetentionPolicyID) > 128 || len(m.Objects) > MaxObjects {
		return ErrInvalid
	}
	if m.Kind != "log" && m.State != "complete" {
		return ErrInvalid
	}
	if m.Kind == "diff" {
		if m.Capture == nil || !ValidHash(m.Capture.Base, 40) || !ValidHash(m.Capture.Head, 40) || !ValidHash(m.Capture.MergeBase, 40) || m.Capture.ContextLines < 0 || m.Capture.ContextLines > 100 || m.Capture.FileContext != "changed_files" || m.Capture.WorkingTree {
			return ErrInvalid
		}
	} else if m.Capture != nil {
		return ErrInvalid
	}
	var total int64
	seen := make(map[string]bool, len(m.Objects))
	for index, obj := range m.Objects {
		if !ValidID(obj.ID, "object") || seen[obj.ID] || !ValidHash(obj.SHA256, 64) || obj.Size < 0 || obj.Size > objectLimit(m.Kind) || !validMedia(m.Kind, obj.MediaType) || !validPath(obj.Path) {
			return ErrInvalid
		}
		if m.Kind == "log" && (obj.Sequence != index || obj.Offset != total) {
			return ErrInvalid
		}
		if !slices.Contains([]string{"", "base", "head", "diff"}, obj.Side) {
			return ErrInvalid
		}
		seen[obj.ID] = true
		total += obj.Size
	}
	if total != m.TotalBytes || total > MaxArtifactBytes {
		return ErrInvalid
	}
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > MaxManifestBytes || int64(len(raw))+total > MaxArtifactBytes {
		return ErrInvalid
	}
	return nil
}

func objectLimit(kind string) int64 {
	if kind == "log" {
		return MaxChunkBytes
	}
	if kind == "video" {
		return MaxVideoBytes
	}
	return MaxTextBytes
}

func validMedia(kind, media string) bool {
	switch kind {
	case "log":
		return media == "text/plain; charset=utf-8"
	case "diff":
		return media == "text/plain; charset=utf-8" || media == "text/x-diff; charset=utf-8"
	case "screenshot":
		return slices.Contains([]string{"image/png", "image/jpeg", "image/webp"}, media)
	case "video":
		return media == "video/mp4" || media == "video/webm"
	default:
		return false
	}
}

func validPath(value string) bool {
	if value == "" {
		return true
	}
	return len(value) <= 4096 && utf8.ValidString(value) && !strings.HasPrefix(value, "/") && value != ".." && !strings.HasPrefix(value, "../") && path.Clean(value) == value && !strings.Contains(value, "\\") && !strings.ContainsFunc(value, unicode.IsControl)
}
