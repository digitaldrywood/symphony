package artifact

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

type Limits struct {
	RelayBytes       int64 `json:"relay_bytes,omitempty"`
	WindowSeconds    int64 `json:"window_seconds,omitempty"`
	TelemetrySeconds int64 `json:"telemetry_seconds,omitempty"`
	RetainedBytes    int64 `json:"retained_bytes"`
	ReservedBytes    int64 `json:"reserved_bytes"`
	ArtifactBytes    int64 `json:"artifact_bytes"`
	UploadBytes      int64 `json:"upload_bytes"`
	RetentionSeconds int64 `json:"retention_seconds"`
}

type Allowances interface {
	Limits(context.Context, string) (Limits, error)
}

type Policy struct {
	ID                     string `json:"id"`
	Limits                 Limits `json:"limits"`
	AbandonedUploadSeconds int64  `json:"abandoned_upload_seconds"`
	DeletionRecordSeconds  int64  `json:"deletion_record_seconds"`
	BackupSeconds          int64  `json:"backup_seconds"`
}

type Config struct {
	ServiceID       string        `json:"service_id"`
	Mode            string        `json:"mode"`
	HostedOptIn     bool          `json:"hosted_opt_in"`
	OrganizationID  string        `json:"organization_id"`
	DatabasePath    string        `json:"database_path"`
	Policy          Policy        `json:"policy"`
	Storage         StorageConfig `json:"storage"`
	HubOrigin       string        `json:"hub_origin"`
	PublishTokenEnv string        `json:"publish_token_env"`
	AllowedOrigins  []string      `json:"allowed_origins"`
}

type Reservation struct {
	Scope
	ServiceID    string `json:"service_id"`
	Mode         string `json:"mode"`
	HostedOptIn  bool   `json:"hosted_opt_in"`
	Key          string `json:"idempotency_key"`
	Kind         string `json:"kind"`
	Bytes        int64  `json:"bytes"`
	LeaseID      string `json:"lease_id"`
	FencingToken int64  `json:"fencing_token"`
}

type Upload struct {
	PartLimit  int64  `json:"part_limit"`
	ArtifactID string `json:"artifact_id"`
	Reservation
	State         string    `json:"state"`
	CreatedAt     time.Time `json:"created_at"`
	Deadline      time.Time `json:"deadline"`
	ExpiresAt     time.Time `json:"expires_at"`
	RetainedBytes int64     `json:"retained_bytes"`
}

type Part struct {
	Sequence  int    `json:"sequence"`
	MediaType string `json:"media_type"`
	Path      string `json:"path,omitempty"`
	Side      string `json:"side,omitempty"`
	SHA256    string `json:"sha256"`
	Data      []byte `json:"data"`
}

type Usage struct {
	WindowStart     time.Time `json:"window_start,omitzero"`
	ObservedAt      time.Time `json:"observed_at,omitzero"`
	RelayBytes      int64     `json:"relay_bytes,omitempty"`
	StorageRequests int64     `json:"storage_requests,omitempty"`
	RetainedBytes   int64     `json:"retained_bytes"`
	ReservedBytes   int64     `json:"reserved_bytes"`
}

type Service struct {
	trafficWindow    atomic.Int64
	trafficRetention atomic.Int64
	config           Config
	storage          Storage
	catalog          *catalog
	allowances       Allowances
	now              func() time.Time
	mu               sync.Mutex
}

func (cfg Config) Validate() error {
	p := cfg.Policy
	if !ValidID(cfg.ServiceID, "service") || !ValidID(cfg.OrganizationID, "org") || !slices.Contains([]string{"customer", "hosted"}, cfg.Mode) || cfg.Mode == "hosted" && !cfg.HostedOptIn || p.ID == "" || len(p.ID) > 128 || !validLimits(p.Limits) || p.AbandonedUploadSeconds <= 0 || p.AbandonedUploadSeconds > 365*86400 || p.BackupSeconds <= 0 || p.BackupSeconds > 365*86400 || p.DeletionRecordSeconds <= p.BackupSeconds+p.AbandonedUploadSeconds || p.DeletionRecordSeconds > 10*365*86400 {
		return ErrInvalid
	}
	return nil
}

func validLimits(l Limits) bool {
	return l.RetainedBytes > 0 && l.RetainedBytes <= 1<<60 && l.ReservedBytes > 0 && l.ReservedBytes <= 1<<60 && l.ArtifactBytes > 0 && l.ArtifactBytes <= MaxArtifactBytes && l.UploadBytes > 0 && l.UploadBytes <= MaxVideoBytes && l.RetentionSeconds > 0 && l.RetentionSeconds <= 10*365*86400
}

func NewService(ctx context.Context, cfg Config, storage Storage, allowances Allowances) (*Service, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if storage == nil {
		return nil, ErrUnsupported
	}
	if cfg.Mode == "hosted" && allowances == nil {
		return nil, ErrQuota
	}
	catalog, err := openCatalog(ctx, cfg.DatabasePath)
	if err != nil {
		return nil, err
	}
	service := &Service{config: cfg, storage: storage, catalog: catalog, allowances: allowances, now: time.Now}
	var window, retention int64
	if err := catalog.db.QueryRowContext(ctx, "SELECT window_seconds,retention_seconds FROM hosted_traffic_settings WHERE singleton=1").Scan(&window, &retention); err != nil {
		return nil, errors.Join(err, catalog.Close())
	}
	service.trafficWindow.Store(window)
	service.trafficRetention.Store(retention)
	if cfg.Mode == "hosted" {
		service.storage = hostedStorage{service: service, storage: storage}
	}
	return service, nil
}

func (s *Service) Close() error { return s.catalog.Close() }

func (s *Service) Backup(ctx context.Context, path string) error { return s.catalog.backup(ctx, path) }

func (s *Service) limits(ctx context.Context) (Limits, error) {
	l := s.config.Policy.Limits
	if s.config.Mode != "hosted" {
		return l, nil
	}
	current, err := s.allowances.Limits(ctx, s.config.OrganizationID)
	if err != nil || !validLimits(current) {
		return Limits{}, ErrQuota
	}
	if err := s.configureTraffic(ctx, current); err != nil {
		return Limits{}, err
	}
	l.RelayBytes = current.RelayBytes
	if s.config.Policy.Limits.RelayBytes > 0 {
		l.RelayBytes = min(l.RelayBytes, s.config.Policy.Limits.RelayBytes)
	}
	l.RetainedBytes = min(l.RetainedBytes, current.RetainedBytes)
	l.ReservedBytes = min(l.ReservedBytes, current.ReservedBytes)
	l.ArtifactBytes = min(l.ArtifactBytes, current.ArtifactBytes)
	l.UploadBytes = min(l.UploadBytes, current.UploadBytes)
	l.RetentionSeconds = min(l.RetentionSeconds, current.RetentionSeconds)
	return l, nil
}

func (s *Service) Usage(ctx context.Context) (Usage, error) {
	var usage Usage
	err := s.catalog.db.QueryRowContext(ctx, "SELECT coalesce(sum(retained_bytes),0),coalesce(sum(reserved_bytes),0) FROM uploads WHERE organization_id=? AND state!='deleted'", s.config.OrganizationID).Scan(&usage.RetainedBytes, &usage.ReservedBytes)
	if err != nil || s.config.Mode != "hosted" {
		return usage, err
	}
	now := s.now().UTC()
	seconds := s.trafficWindow.Load()
	usage.WindowStart = time.Unix(now.Unix()/seconds*seconds, 0).UTC()
	usage.ObservedAt = now
	err = s.catalog.db.QueryRowContext(ctx, "SELECT coalesce(sum(upload_bytes+download_bytes),0),coalesce(sum(requests),0) FROM hosted_traffic WHERE minute >= ?", usage.WindowStart.Unix()).Scan(&usage.RelayBytes, &usage.StorageRequests)
	return usage, err
}

func (s *Service) Reserve(ctx context.Context, r Reservation) (Upload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ServiceID != s.config.ServiceID || r.Mode != s.config.Mode || r.HostedOptIn != s.config.HostedOptIn {
		return Upload{}, ErrDenied
	}
	if r.Validate() != nil || r.OrganizationID != s.config.OrganizationID || r.Key == "" || len(r.Key) > 128 || !slices.Contains([]string{"log", "diff", "screenshot", "video"}, r.Kind) || r.Bytes <= 0 || r.Bytes > MaxArtifactBytes {
		return Upload{}, ErrInvalid
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return Upload{}, err
	}
	var id, hash string
	err = s.catalog.db.QueryRowContext(ctx, "SELECT id,request_hash FROM uploads WHERE organization_id=? AND project_id=? AND idempotency_key=?", r.OrganizationID, r.ProjectID, r.Key).Scan(&id, &hash)
	if err == nil {
		if hash != Digest(raw) {
			return Upload{}, ErrConflict
		}
		return s.Upload(ctx, id)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Upload{}, err
	}
	l, err := s.limits(ctx)
	if err != nil {
		return Upload{}, err
	}
	u, err := s.Usage(ctx)
	if err != nil {
		return Upload{}, err
	}
	if r.Bytes > l.ArtifactBytes || r.Bytes > l.ReservedBytes-u.ReservedBytes || r.Bytes > l.RetainedBytes-u.RetainedBytes-u.ReservedBytes {
		return Upload{}, ErrQuota
	}
	if s.config.Mode == "hosted" && (r.Bytes > (l.RelayBytes-u.RelayBytes)/2-u.ReservedBytes) {
		return Upload{}, ErrQuota
	}
	now := s.now().UTC().Truncate(time.Second)
	upload := Upload{PartLimit: l.UploadBytes, ArtifactID: NewID("artifact"), Reservation: r, State: "uploading", CreatedAt: now, Deadline: now.Add(time.Duration(s.config.Policy.AbandonedUploadSeconds) * time.Second), ExpiresAt: now.Add(time.Duration(l.RetentionSeconds) * time.Second)}
	_, err = s.catalog.db.ExecContext(ctx, "INSERT INTO uploads(id,organization_id,project_id,idempotency_key,request_hash,request_json,state,reserved_bytes,created_at,upload_deadline,expires_at,admitted_upload_bytes) VALUES(?,?,?,?,?,?,'uploading',?,?,?,?,?)", upload.ArtifactID, r.OrganizationID, r.ProjectID, r.Key, Digest(raw), raw, r.Bytes, now.Unix(), upload.Deadline.Unix(), upload.ExpiresAt.Unix(), l.UploadBytes)
	return upload, err
}

func (s *Service) loadUpload(ctx context.Context, id string) (Upload, error) {
	var u Upload
	var raw []byte
	var created, deadline, expires int64
	err := s.catalog.db.QueryRowContext(ctx, "SELECT request_json,state,created_at,upload_deadline,expires_at,retained_bytes,admitted_upload_bytes FROM uploads WHERE id=?", id).Scan(&raw, &u.State, &created, &deadline, &expires, &u.RetainedBytes, &u.PartLimit)
	if errors.Is(err, sql.ErrNoRows) {
		return u, ErrMissing
	}
	if err != nil {
		return u, err
	}
	if err := json.Unmarshal(raw, &u.Reservation); err != nil {
		return u, err
	}
	if u.ServiceID != s.config.ServiceID || u.Mode != s.config.Mode || u.HostedOptIn != s.config.HostedOptIn {
		return u, ErrDenied
	}
	u.ArtifactID = id
	u.CreatedAt, u.Deadline, u.ExpiresAt = time.Unix(created, 0).UTC(), time.Unix(deadline, 0).UTC(), time.Unix(expires, 0).UTC()
	return u, nil
}

func (s *Service) Upload(ctx context.Context, id string) (Upload, error) {
	u, err := s.loadUpload(ctx, id)
	if err != nil {
		return u, err
	}
	if u.State != "deleted" && u.State != "deletion_pending" {
		if _, err := s.storage.Get(ctx, id+"/deleted", "", 128); err == nil {
			if _, err := s.catalog.db.ExecContext(ctx, "UPDATE uploads SET state='deletion_pending' WHERE id=? AND state!='deleted'", id); err != nil {
				return u, err
			}
			u.State = "deletion_pending"
		} else if !errors.Is(err, ErrMissing) {
			return u, err
		}
	}
	return u, nil
}

func (s *Service) Append(ctx context.Context, id string, p Part) (Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, err := s.Upload(ctx, id)
	if err != nil {
		return Object{}, err
	}
	if !s.now().Before(u.ExpiresAt) {
		return Object{}, ErrExpired
	}
	if u.State == "deleted" || u.State == "deletion_pending" {
		return Object{}, ErrMissing
	}
	if p.Sequence < 0 || p.Sequence >= MaxObjects || !validMedia(u.Kind, p.MediaType) || !validPath(p.Path) || int64(len(p.Data)) > min(objectLimit(u.Kind), s.config.Policy.Limits.UploadBytes) || !slices.Contains([]string{"", "base", "head", "diff"}, p.Side) {
		return Object{}, ErrInvalid
	}
	if Digest(p.Data) != p.SHA256 {
		return Object{}, ErrIntegrity
	}
	if (u.Kind == "log" || u.Kind == "diff") && !utf8.Valid(p.Data) {
		return Object{}, ErrInvalid
	}
	if err := validateMedia(p.MediaType, p.Data); err != nil {
		return Object{}, err
	}
	obj := Object{ID: NewID("object"), MediaType: p.MediaType, Size: int64(len(p.Data)), SHA256: p.SHA256, Path: p.Path, Side: p.Side, Sequence: p.Sequence}
	var existing []byte
	var version string
	var verified int
	err = s.catalog.db.QueryRowContext(ctx, "SELECT descriptor_json,storage_version,verified FROM objects WHERE upload_id=? AND sequence=?", id, p.Sequence).Scan(&existing, &version, &verified)
	if err == nil {
		var previous Object
		if err := json.Unmarshal(existing, &previous); err != nil {
			return Object{}, err
		}
		obj.ID, obj.Offset = previous.ID, previous.Offset
		if obj != previous {
			return Object{}, ErrConflict
		}
		if verified == 1 {
			if u.Kind == "log" && u.State == "uploading" {
				var count int
				if err := s.catalog.db.QueryRowContext(ctx, "SELECT coalesce(max(json_extract(body,'$.objects[#-1].sequence')),-1) FROM manifests WHERE upload_id=?", id).Scan(&count); err != nil {
					return Object{}, err
				}
				if count < p.Sequence {
					if _, err := s.seal(ctx, id, "partial", nil); err != nil {
						return Object{}, err
					}
				}
			}
			return obj, nil
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if u.State != "uploading" {
			return Object{}, ErrConflict
		}
		if !s.now().Before(u.Deadline) {
			return Object{}, ErrExpired
		}
		partLimit := u.PartLimit
		if partLimit == 0 {
			partLimit = s.config.Policy.Limits.UploadBytes
		}
		if int64(len(p.Data)) > partLimit {
			return Object{}, ErrQuota
		}
		var count int
		if err := s.catalog.db.QueryRowContext(ctx, "SELECT count(*) FROM objects WHERE upload_id=? AND verified=1", id).Scan(&count); err != nil {
			return Object{}, err
		}
		if count != p.Sequence {
			return Object{}, ErrConflict
		}
		var total int64
		objects, err := s.objects(ctx, id)
		if err != nil {
			return Object{}, err
		}
		for _, o := range objects {
			total += o.Size
		}
		obj.Offset = total
		descriptors, err := json.Marshal(append(objects, obj))
		if err != nil {
			return Object{}, err
		}
		headroom := int64(2048 + len(descriptors))
		if headroom > MaxManifestBytes {
			return Object{}, ErrQuota
		}
		if u.Kind == "log" {
			headroom *= 2
		}
		if obj.Size > u.Bytes-u.RetainedBytes-headroom {
			return Object{}, ErrQuota
		}
		raw, err := json.Marshal(obj)
		if err != nil {
			return Object{}, err
		}
		if _, err := s.catalog.db.ExecContext(ctx, "INSERT INTO objects(upload_id,sequence,id,descriptor_json) VALUES(?,?,?,?)", id, p.Sequence, obj.ID, raw); err != nil {
			return Object{}, err
		}
	} else {
		return Object{}, err
	}
	if u.State != "uploading" {
		return Object{}, ErrConflict
	}
	if !s.now().Before(u.Deadline) {
		return Object{}, ErrExpired
	}
	key := id + "/" + obj.ID
	version, err = s.storage.Put(ctx, key, p.Data)
	if err != nil && !errors.Is(err, ErrConflict) {
		return Object{}, err
	}
	read, err := s.storage.Get(ctx, key, version, obj.Size)
	if err != nil {
		return Object{}, err
	}
	if int64(len(read)) != obj.Size || Digest(read) != obj.SHA256 {
		return Object{}, ErrIntegrity
	}
	tx, err := s.catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return Object{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE objects SET storage_version=?,verified=1 WHERE id=?", version, obj.ID); err != nil {
		return Object{}, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE uploads SET retained_bytes=retained_bytes+?,reserved_bytes=reserved_bytes-? WHERE id=?", obj.Size, obj.Size, id); err != nil {
		return Object{}, err
	}
	if err := tx.Commit(); err != nil {
		return Object{}, err
	}
	if u.Kind == "log" {
		if _, err := s.seal(ctx, id, "partial", nil); err != nil {
			return obj, err
		}
	}
	return obj, nil
}

func (s *Service) objects(ctx context.Context, id string) ([]Object, error) {
	rows, err := s.catalog.db.QueryContext(ctx, "SELECT descriptor_json FROM objects WHERE upload_id=? AND verified=1 ORDER BY sequence", id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := []Object{}
	for rows.Next() {
		var raw []byte
		var obj Object
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

func (s *Service) Finalize(ctx context.Context, id, state string, capture *Capture) (Reference, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state != "complete" && state != "interrupted" {
		return Reference{}, ErrInvalid
	}
	return s.seal(ctx, id, state, capture)
}

func (s *Service) seal(ctx context.Context, id, state string, capture *Capture) (Reference, error) {
	u, err := s.Upload(ctx, id)
	if err != nil {
		return Reference{}, err
	}
	if !s.now().Before(u.ExpiresAt) {
		return Reference{}, ErrExpired
	}
	if u.State != "uploading" {
		if u.State == state {
			body, ref, err := s.Manifest(ctx, id, 0)
			if err != nil {
				return Reference{}, err
			}
			var previous Manifest
			if err := json.Unmarshal(body, &previous); err != nil {
				return Reference{}, ErrIntegrity
			}
			if (capture == nil) != (previous.Capture == nil) || capture != nil && *capture != *previous.Capture {
				return Reference{}, ErrConflict
			}
			return ref, err
		}
		return Reference{}, ErrConflict
	}
	if state == "complete" && !s.now().Before(u.Deadline) {
		return Reference{}, ErrExpired
	}
	objects, err := s.objects(ctx, id)
	if err != nil {
		return Reference{}, err
	}
	var pending int
	if err := s.catalog.db.QueryRowContext(ctx, "SELECT count(*) FROM objects WHERE upload_id=? AND verified=0", id).Scan(&pending); err != nil {
		return Reference{}, err
	}
	if pending != 0 && state == "complete" {
		return Reference{}, ErrConflict
	}
	if pending != 0 && state == "interrupted" {
		if err := s.discardPending(ctx, id); err != nil {
			return Reference{}, err
		}
	}
	var revision int64
	if err := s.catalog.db.QueryRowContext(ctx, "SELECT coalesce(max(revision),0)+1 FROM manifests WHERE upload_id=?", id).Scan(&revision); err != nil {
		return Reference{}, err
	}
	m := Manifest{SchemaVersion: 1, Scope: u.Scope, ArtifactID: id, ManifestID: NewID("manifest"), Revision: revision, Kind: u.Kind, State: state, CreatedAt: u.CreatedAt, ExpiresAt: u.ExpiresAt, RetentionPolicyID: s.config.Policy.ID, Objects: objects, Capture: capture}
	for _, obj := range objects {
		m.TotalBytes += obj.Size
	}
	if err := m.Validate(); err != nil {
		return Reference{}, err
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return Reference{}, err
	}
	if int64(len(raw)) > u.Bytes-u.RetainedBytes {
		return Reference{}, ErrQuota
	}
	ref := Reference{SchemaVersion: 1, Scope: m.Scope, ServiceID: s.config.ServiceID, ArtifactID: id, ManifestID: m.ManifestID, Revision: revision, SHA256: Digest(raw), Kind: m.Kind, State: state, Availability: "available", Bytes: m.TotalBytes, Objects: len(objects), ExpiresAt: m.ExpiresAt, ObservedAt: s.now().UTC()}
	encoded, err := json.Marshal(ref)
	if err != nil {
		return Reference{}, err
	}
	tx, err := s.catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return Reference{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO manifests(upload_id,revision,id,body,digest,reference_json) VALUES(?,?,?,?,?,?)", id, revision, m.ManifestID, raw, ref.SHA256, encoded); err != nil {
		return Reference{}, err
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO outbox(manifest_id) VALUES(?)", m.ManifestID); err != nil {
		return Reference{}, err
	}
	newState := u.State
	if state != "partial" {
		newState = state
	}
	if _, err := tx.ExecContext(ctx, "UPDATE uploads SET state=?,retained_bytes=retained_bytes+?,reserved_bytes=CASE WHEN ?='uploading' THEN reserved_bytes-? ELSE 0 END WHERE id=?", newState, len(raw), newState, len(raw), id); err != nil {
		return Reference{}, err
	}
	return ref, tx.Commit()
}

func (s *Service) Manifest(ctx context.Context, id string, revision int64) ([]byte, Reference, error) {
	u, err := s.Upload(ctx, id)
	if err != nil {
		return nil, Reference{}, err
	}
	if !s.now().Before(u.ExpiresAt) {
		return nil, Reference{}, ErrExpired
	}
	if u.State == "deleted" || u.State == "deletion_pending" {
		return nil, Reference{}, ErrMissing
	}
	var body, raw []byte
	err = s.catalog.db.QueryRowContext(ctx, "SELECT body,reference_json FROM manifests WHERE upload_id=? AND (?=0 OR revision=?) ORDER BY revision DESC LIMIT 1", id, revision, revision).Scan(&body, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, Reference{}, ErrMissing
	}
	if err != nil {
		return nil, Reference{}, err
	}
	var ref Reference
	if err := json.Unmarshal(raw, &ref); err != nil {
		return nil, ref, err
	}
	if Digest(body) != ref.SHA256 {
		return nil, ref, ErrIntegrity
	}
	return body, ref, nil
}

func (s *Service) ReadObject(ctx context.Context, id string, revision int64, objectID string) (Object, []byte, error) {
	body, _, err := s.Manifest(ctx, id, revision)
	if err != nil {
		return Object{}, nil, err
	}
	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return Object{}, nil, ErrIntegrity
	}
	for _, obj := range m.Objects {
		if obj.ID != objectID {
			continue
		}
		var version string
		if err := s.catalog.db.QueryRowContext(ctx, "SELECT storage_version FROM objects WHERE upload_id=? AND id=? AND verified=1", id, objectID).Scan(&version); err != nil {
			return obj, nil, ErrMissing
		}
		data, err := s.storage.Get(ctx, id+"/"+objectID, version, obj.Size)
		if err != nil {
			return obj, nil, err
		}
		if int64(len(data)) != obj.Size || Digest(data) != obj.SHA256 {
			return obj, nil, ErrIntegrity
		}
		return obj, data, nil
	}
	return Object{}, nil, ErrMissing
}
