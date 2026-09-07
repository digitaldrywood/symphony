package artifact

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type memoryStorage struct {
	mu        sync.Mutex
	data      map[string][]byte
	failure   error
	lostReply bool
}

func (m *memoryStorage) Put(_ context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return "", m.failure
	}
	if _, ok := m.data[key]; ok {
		return "version", ErrConflict
	}
	m.data[key] = bytes.Clone(data)
	if m.lostReply {
		m.lostReply = false
		return "", ErrStorage
	}
	return "version", nil
}

func (m *memoryStorage) Get(_ context.Context, key, _ string, limit int64) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return nil, m.failure
	}
	data, ok := m.data[key]
	if !ok {
		return nil, ErrMissing
	}
	if int64(len(data)) > limit {
		return nil, ErrIntegrity
	}
	return bytes.Clone(data), nil
}

func (m *memoryStorage) Delete(_ context.Context, key, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failure != nil {
		return m.failure
	}
	delete(m.data, key)
	return nil
}

type testAllowances struct{ limits Limits }

func (a *testAllowances) Limits(context.Context, string) (Limits, error) { return a.limits, nil }

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{ServiceID: NewID("service"), OrganizationID: NewID("org"), Mode: "customer", DatabasePath: filepath.Join(t.TempDir(), "catalog.db"), Policy: Policy{ID: "test", Limits: Limits{RetainedBytes: 64 << 20, ReservedBytes: 32 << 20, ArtifactBytes: 8 << 20, UploadBytes: MaxChunkBytes, RetentionSeconds: 3600}, AbandonedUploadSeconds: 600, DeletionRecordSeconds: 7200, BackupSeconds: 3600}}
}

func testService(t *testing.T) (*Service, *memoryStorage) {
	t.Helper()
	storage := &memoryStorage{data: map[string][]byte{}}
	s, err := NewService(t.Context(), testConfig(t), storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, storage
}

func testReservation(s *Service) Reservation {
	return Reservation{ServiceID: s.config.ServiceID, Mode: s.config.Mode, HostedOptIn: s.config.HostedOptIn, Scope: Scope{OrganizationID: s.config.OrganizationID, ProjectID: NewID("prj"), WorkItemID: NewID("wi"), RunID: NewID("run"), AttemptID: NewID("attempt")}, Key: "log", Kind: "log", Bytes: 4 << 20, LeaseID: "lease", FencingToken: 1}
}

func testPart(sequence int, data string) Part {
	return Part{Sequence: sequence, MediaType: "text/plain; charset=utf-8", SHA256: Digest([]byte(data)), Data: []byte(data)}
}

func TestUploadRecoveryAndImmutableManifests(t *testing.T) {
	t.Parallel()
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_reply_%t", lost), func(t *testing.T) {
			s, storage := testService(t)
			r := testReservation(s)
			u, err := s.Reserve(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			retry, err := s.Reserve(t.Context(), r)
			if err != nil || retry.ArtifactID != u.ArtifactID {
				t.Fatalf("reserve retry: %v %#v", err, retry)
			}
			changed := r
			changed.Bytes--
			if _, err := s.Reserve(t.Context(), changed); !errors.Is(err, ErrConflict) {
				t.Fatalf("changed reserve: %v", err)
			}
			storage.lostReply = lost
			part := testPart(0, "<script>customer log</script>\n")
			if lost {
				if _, err := s.Append(t.Context(), u.ArtifactID, part); !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
			}
			obj, err := s.Append(t.Context(), u.ArtifactID, part)
			if err != nil {
				t.Fatal(err)
			}
			usage, err := s.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(t.Context(), u.ArtifactID, part); err != nil {
				t.Fatal(err)
			}
			again, err := s.Usage(t.Context())
			if err != nil || again != usage {
				t.Fatalf("duplicate charged: %v %v %v", usage, again, err)
			}
			partial, pref, err := s.Manifest(t.Context(), u.ArtifactID, 1)
			if err != nil || pref.State != "partial" {
				t.Fatalf("partial: %v %v", pref, err)
			}
			ref, err := s.Finalize(t.Context(), u.ArtifactID, "complete", nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(t.Context(), u.ArtifactID, part); err != nil {
				t.Fatalf("retry completed part: %v", err)
			}
			if _, err := s.Append(t.Context(), u.ArtifactID, testPart(1, "later")); !errors.Is(err, ErrConflict) {
				t.Fatalf("append completed: %v", err)
			}
			final, err := s.Finalize(t.Context(), u.ArtifactID, "complete", nil)
			if err != nil || final != ref {
				t.Fatalf("finalize retry: %v", err)
			}
			old, _, err := s.Manifest(t.Context(), u.ArtifactID, 1)
			if err != nil || !bytes.Equal(old, partial) {
				t.Fatal("partial mutated", err)
			}
			_, data, err := s.ReadObject(t.Context(), u.ArtifactID, ref.Revision, obj.ID)
			if err != nil || !bytes.Equal(data, part.Data) {
				t.Fatal("read", err)
			}
			if _, err := s.catalog.db.ExecContext(t.Context(), "UPDATE manifests SET digest='changed'"); err == nil {
				t.Fatal("manifest update allowed")
			}
			usage, err = s.Usage(t.Context())
			if err != nil || usage.ReservedBytes != 0 || usage.RetainedBytes <= int64(len(part.Data)) {
				t.Fatal("accounting", usage, err)
			}
			cfg := s.config
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewService(t.Context(), cfg, storage, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			_, data, err = reopened.ReadObject(t.Context(), u.ArtifactID, ref.Revision, obj.ID)
			if err != nil || !bytes.Equal(data, part.Data) {
				t.Fatal("restart", err)
			}
			publisher := &testPublisher{failure: ErrStorage}
			if err := reopened.PublishPending(t.Context(), publisher); !errors.Is(err, ErrStorage) {
				t.Fatal(err)
			}
			publisher.failure = nil
			if err := reopened.PublishPending(t.Context(), publisher); err != nil {
				t.Fatal(err)
			}
			if len(publisher.refs) != 2 {
				t.Fatalf("outbox: %d", len(publisher.refs))
			}
			if err := reopened.PublishPending(t.Context(), publisher); err != nil || len(publisher.refs) != 2 {
				t.Fatal("outbox duplicate", err)
			}
		})
	}
}

type testPublisher struct {
	failure error
	refs    []Reference
}

func (p *testPublisher) Publish(_ context.Context, ref Reference) error {
	if p.failure != nil {
		return p.failure
	}
	p.refs = append(p.refs, ref)
	return nil
}

func TestArtifactFailureStates(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*Service, *memoryStorage, Upload, Object)
		want   error
	}{
		{"missing", func(_ *Service, m *memoryStorage, u Upload, o Object) { delete(m.data, u.ArtifactID+"/"+o.ID) }, ErrMissing},
		{"corrupt", func(_ *Service, m *memoryStorage, u Upload, o Object) { m.data[u.ArtifactID+"/"+o.ID] = []byte("bad") }, ErrIntegrity},
		{"outage", func(_ *Service, m *memoryStorage, _ Upload, _ Object) { m.failure = ErrStorage }, ErrStorage},
		{"expired", func(s *Service, _ *memoryStorage, u Upload, _ Object) {
			s.now = func() time.Time { return u.ExpiresAt }
		}, ErrExpired},
		{"deleted", func(s *Service, _ *memoryStorage, u Upload, _ Object) {
			if err := s.Delete(t.Context(), u.ArtifactID); err != nil {
				t.Error(err)
			}
		}, ErrMissing},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, m := testService(t)
			u, err := s.Reserve(t.Context(), testReservation(s))
			if err != nil {
				t.Fatal(err)
			}
			o, err := s.Append(t.Context(), u.ArtifactID, testPart(0, "log"))
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(s, m, u, o)
			_, _, err = s.ReadObject(t.Context(), u.ArtifactID, 1, o.ID)
			if !errors.Is(err, test.want) {
				t.Fatalf("got %v want %v", err, test.want)
			}
		})
	}
}

func TestHostedConcurrentReservationsAndDowngrade(t *testing.T) {
	t.Parallel()
	cfg := testConfig(t)
	cfg.Mode = "hosted"
	cfg.HostedOptIn = true
	allow := &testAllowances{cfg.Policy.Limits}
	allow.limits.RetainedBytes = 4 << 20
	allow.limits.ReservedBytes = 4 << 20
	s, err := NewService(t.Context(), cfg, &memoryStorage{data: map[string][]byte{}}, allow)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := testReservation(s)
	var wg sync.WaitGroup
	results := make(chan error, 8)
	for i := range 8 {
		wg.Go(func() { r := r; r.Key = strconv.Itoa(i); _, err := s.Reserve(t.Context(), r); results <- err })
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrQuota) {
			t.Fatal(err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners %d", winners)
	}
	allow.limits.RetainedBytes = 1
	r.Key = "downgrade"
	if _, err := s.Reserve(t.Context(), r); !errors.Is(err, ErrQuota) {
		t.Fatal(err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil || usage.ReservedBytes != 4<<20 {
		t.Fatal("downgrade lost reservation", usage, err)
	}
}

func TestAbandonedUploadsAndDeletionRecovery(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"log", "diff"} {
		t.Run(kind, func(t *testing.T) {
			s, m := testService(t)
			r := testReservation(s)
			r.Kind = kind
			u, err := s.Reserve(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(t.Context(), u.ArtifactID, testPart(0, "log")); err != nil {
				t.Fatal(err)
			}
			s.now = func() time.Time { return u.Deadline }
			if err := s.Maintain(t.Context()); err != nil {
				t.Fatal(err)
			}
			updated, err := s.Upload(t.Context(), u.ArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			want := "deleted"
			if kind == "log" {
				want = "interrupted"
			}
			if updated.State != want {
				t.Fatalf("%s want %s", updated.State, want)
			}
			s.now = func() time.Time { return u.ExpiresAt }
			m.failure = ErrStorage
			if kind == "log" {
				if err := s.Maintain(t.Context()); !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
			}
			m.failure = nil
			if err := s.Maintain(t.Context()); err != nil {
				t.Fatal(err)
			}
			usage, err := s.Usage(t.Context())
			if err != nil || usage != (Usage{}) {
				t.Fatal(usage, err)
			}
			s.now = func() time.Time {
				return u.ExpiresAt.Add(time.Duration(s.config.Policy.DeletionRecordSeconds+1) * time.Second)
			}
			if err := s.Maintain(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Upload(t.Context(), u.ArtifactID); !errors.Is(err, ErrMissing) {
				t.Fatal(err)
			}
		})
	}
}

func TestCatalogOwnershipAndBackup(t *testing.T) {
	t.Parallel()
	s, m := testService(t)
	if other, err := NewService(t.Context(), s.config, m, nil); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	u, err := s.Reserve(t.Context(), testReservation(s))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(t.Context(), u.ArtifactID, testPart(0, "backup")); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := s.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := s.Backup(t.Context(), backup); err == nil {
		t.Fatal("existing backup overwritten")
	}
	cfg := s.config
	cfg.DatabasePath = backup
	restored, err := NewService(t.Context(), cfg, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	var integrity string
	if err := restored.catalog.db.QueryRowContext(t.Context(), "PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal(integrity, err)
	}
	body, _, err := restored.Manifest(t.Context(), u.ArtifactID, 1)
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Validate() != nil {
		t.Fatal("restored manifest invalid")
	}
}

func TestCatalogRejectsInvalidDatabases(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"memory", "uri", "wrong application", "future version"} {
		t.Run(kind, func(t *testing.T) {
			cfg := testConfig(t)
			switch kind {
			case "memory":
				cfg.DatabasePath = ":memory:"
			case "uri":
				cfg.DatabasePath = "file:catalog.db"
			case "wrong application", "future version":
				if kind == "future version" {
					s, err := NewService(t.Context(), cfg, &memoryStorage{data: map[string][]byte{}}, nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
				}
				db, err := sql.Open("sqlite", cfg.DatabasePath)
				if err != nil {
					t.Fatal(err)
				}
				query := "PRAGMA application_id=123"
				if kind == "future version" {
					query = "INSERT INTO artifact_schema_version(version_id,is_applied) VALUES(99,1)"
				}
				if _, err := db.ExecContext(t.Context(), query); err != nil {
					t.Fatal(err)
				}
				db.Close()
			}
			if s, err := NewService(t.Context(), cfg, &memoryStorage{data: map[string][]byte{}}, nil); err == nil {
				s.Close()
				t.Fatal("invalid catalog accepted")
			}
		})
	}
}

func TestAbandonedUnverifiedObjectKeepsReservationUntilCleanup(t *testing.T) {
	t.Parallel()
	for _, outage := range []bool{false, true} {
		t.Run(strconv.FormatBool(outage), func(t *testing.T) {
			s, storage := testService(t)
			u, err := s.Reserve(t.Context(), testReservation(s))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Append(t.Context(), u.ArtifactID, testPart(0, "verified")); err != nil {
				t.Fatal(err)
			}
			storage.lostReply = true
			if _, err := s.Append(t.Context(), u.ArtifactID, testPart(1, "unverified")); !errors.Is(err, ErrStorage) {
				t.Fatal(err)
			}
			s.now = func() time.Time { return u.Deadline }
			if outage {
				storage.failure = ErrStorage
				if err := s.Maintain(t.Context()); !errors.Is(err, ErrStorage) {
					t.Fatal(err)
				}
				usage, err := s.Usage(t.Context())
				if err != nil || usage.ReservedBytes == 0 {
					t.Fatal("reservation released during outage", usage, err)
				}
				storage.failure = nil
			}
			if err := s.Maintain(t.Context()); err != nil {
				t.Fatal(err)
			}
			_, ref, err := s.Manifest(t.Context(), u.ArtifactID, 0)
			if err != nil || ref.State != "interrupted" || ref.Objects != 1 {
				t.Fatal(ref, err)
			}
			if len(storage.data) != 1 {
				t.Fatal("unverified object retained", len(storage.data))
			}
			usage, err := s.Usage(t.Context())
			if err != nil || usage.ReservedBytes != 0 {
				t.Fatal(usage, err)
			}
		})
	}
}

func TestRestoredCatalogCannotResurrectDeletedArtifact(t *testing.T) {
	t.Parallel()
	s, storage := testService(t)
	r := testReservation(s)
	u, err := s.Reserve(t.Context(), r)
	if err != nil {
		t.Fatal(err)
	}
	p := testPart(0, "must stay deleted")
	if _, err := s.Append(t.Context(), u.ArtifactID, p); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(t.TempDir(), "before-deletion.db")
	if err := s.Backup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(t.Context(), u.ArtifactID); err != nil {
		t.Fatal(err)
	}
	cfg := s.config
	cfg.DatabasePath = backup
	restored, err := NewService(t.Context(), cfg, storage, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	for _, operation := range []struct {
		name string
		run  func() error
	}{
		{"manifest", func() error { _, _, err := restored.Manifest(t.Context(), u.ArtifactID, 1); return err }},
		{"append", func() error { _, err := restored.Append(t.Context(), u.ArtifactID, p); return err }},
		{"finalize", func() error { _, err := restored.Finalize(t.Context(), u.ArtifactID, "complete", nil); return err }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); err == nil {
				t.Fatal("deleted artifact resurrected")
			}
		})
	}
	if err := restored.Maintain(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactManifestHeadroom(t *testing.T) {
	t.Parallel()
	for _, size := range []int64{4096, 16384} {
		t.Run(strconv.FormatInt(size, 10), func(t *testing.T) {
			s, _ := testService(t)
			r := testReservation(s)
			r.Kind, r.Bytes = "diff", size
			u, err := s.Reserve(t.Context(), r)
			if err != nil {
				t.Fatal(err)
			}
			p := testPart(0, "x")
			p.Path, p.Side = strings.Repeat("a", 4096), "head"
			_, err = s.Append(t.Context(), u.ArtifactID, p)
			if size == 4096 {
				if !errors.Is(err, ErrQuota) {
					t.Fatal("manifest headroom not reserved", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			sha := strings.Repeat("a", 40)
			if _, err := s.Finalize(t.Context(), u.ArtifactID, "complete", &Capture{Base: sha, Head: sha, MergeBase: sha, ContextLines: 3, FileContext: "changed_files"}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestReservationCustodyBinding(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"customer", "hosted"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.Mode, cfg.HostedOptIn = mode, mode == "hosted"
			storage := &memoryStorage{data: map[string][]byte{}}
			s, err := NewService(t.Context(), cfg, storage, &testAllowances{cfg.Policy.Limits})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for _, test := range []struct {
				name string
				edit func(*Reservation)
			}{
				{"different service", func(r *Reservation) { r.ServiceID = NewID("service") }},
				{"different mode", func(r *Reservation) { r.Mode = "local" }},
				{"different consent", func(r *Reservation) { r.HostedOptIn = !r.HostedOptIn }},
			} {
				t.Run(test.name, func(t *testing.T) {
					r := testReservation(s)
					test.edit(&r)
					if _, err := s.Reserve(t.Context(), r); !errors.Is(err, ErrDenied) {
						t.Fatal("mismatched custody accepted", err)
					}
				})
			}
			usage, err := s.Usage(t.Context())
			if err != nil || usage != (Usage{}) || len(storage.data) != 0 {
				t.Fatal("mismatched custody created stored data", usage, err)
			}
			upload, err := s.Reserve(t.Context(), testReservation(s))
			if err != nil {
				t.Fatal(err)
			}
			s.config.ServiceID = NewID("service")
			if _, err := s.Upload(t.Context(), upload.ArtifactID); !errors.Is(err, ErrDenied) {
				t.Fatal("existing catalog silently changed service custody", err)
			}
		})
	}
}

func TestCatalogDSNPaths(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, path, want string
	}{
		{"Unix", "/var/lib/artifacts/catalog.db", "file:///var/lib/artifacts/catalog.db?"},
		{"Windows uppercase", "C:/artifacts/catalog.db", "file:///C:/artifacts/catalog.db?"},
		{"Windows lowercase", "d:/artifacts/catalog.db", "file:///d:/artifacts/catalog.db?"},
		{"special characters", "C:/artifact data/catalog#1.db", "file:///C:/artifact%20data/catalog%231.db?"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dsn := catalogDSN(test.path)
			if !strings.HasPrefix(dsn, test.want) {
				t.Fatalf("catalogDSN() = %q, want prefix %q", dsn, test.want)
			}
			parsed, err := url.Parse(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Host != "" || len(parsed.Query()["_pragma"]) != 4 {
				t.Fatal("catalog path became an authority or lost its pragmas", dsn)
			}
		})
	}
}
