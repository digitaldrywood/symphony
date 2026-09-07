package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
)

type Publisher interface {
	Publish(context.Context, Reference) error
}

func (s *Service) discardPending(ctx context.Context, id string) error {
	var objectID, version string
	err := s.catalog.db.QueryRowContext(ctx, "SELECT id,storage_version FROM objects WHERE upload_id=? AND verified=0", id).Scan(&objectID, &version)
	if err != nil {
		return err
	}
	key := id + "/" + objectID
	if err := s.storage.Delete(ctx, key, version); err != nil {
		return err
	}
	if _, err := s.storage.Get(ctx, key, version, 0); !errors.Is(err, ErrMissing) {
		return errors.Join(ErrStorage, err)
	}
	_, err = s.catalog.db.ExecContext(ctx, "DELETE FROM objects WHERE upload_id=? AND id=? AND verified=0", id, objectID)
	return err
}

func (s *Service) PublishPending(ctx context.Context, publisher Publisher) error {
	rows, err := s.catalog.db.QueryContext(ctx, "SELECT m.reference_json FROM outbox o JOIN manifests m ON m.id=o.manifest_id JOIN uploads u ON u.id=m.upload_id WHERE o.delivered=0 AND u.state NOT IN ('deletion_pending','deleted') AND u.expires_at>? ORDER BY u.created_at,m.revision LIMIT 32", s.now().Unix())
	if err != nil {
		return err
	}
	defer rows.Close()
	refs := []Reference{}
	for rows.Next() {
		var raw []byte
		var ref Reference
		if err := rows.Scan(&raw); err != nil {
			return errors.Join(err, rows.Close())
		}
		if err := json.Unmarshal(raw, &ref); err != nil {
			return errors.Join(err, rows.Close())
		}
		refs = append(refs, ref)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, ref := range refs {
		if err := publisher.Publish(ctx, ref); err != nil {
			return err
		}
		if _, err := s.catalog.db.ExecContext(ctx, "UPDATE outbox SET delivered=1 WHERE manifest_id=?", ref.ManifestID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) Maintain(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.catalog.db.QueryContext(ctx, "SELECT id FROM uploads WHERE state!='deleted' AND (expires_at<=? OR (state='uploading' AND upload_deadline<=?) OR state='deletion_pending') ORDER BY created_at LIMIT 32", s.now().Unix(), s.now().Unix())
	if err != nil {
		return err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, id := range ids {
		u, err := s.Upload(ctx, id)
		if err != nil {
			return err
		}
		if u.State == "uploading" && s.now().Before(u.ExpiresAt) && u.Kind == "log" {
			if _, err := s.seal(ctx, id, "interrupted", nil); err != nil {
				return err
			}
			continue
		}
		if err := s.delete(ctx, id); err != nil {
			return err
		}
	}
	cutoff := s.now().Unix() - s.config.Policy.DeletionRecordSeconds
	rows, err = s.catalog.db.QueryContext(ctx, "SELECT id FROM uploads WHERE state='deleted' AND deleted_at<? LIMIT 32", cutoff)
	if err != nil {
		return err
	}
	defer rows.Close()
	ids = nil
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return errors.Join(err, rows.Close())
		}
		ids = append(ids, id)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, id := range ids {
		if err := s.storage.Delete(ctx, id+"/deleted", ""); err != nil {
			return err
		}
	}
	tx, err := s.catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{
		"DELETE FROM outbox WHERE manifest_id IN (SELECT m.id FROM manifests m JOIN uploads u ON u.id=m.upload_id WHERE u.state='deleted' AND u.deleted_at<?)",
		"DELETE FROM manifests WHERE upload_id IN (SELECT id FROM uploads WHERE state='deleted' AND deleted_at<?)",
		"DELETE FROM objects WHERE upload_id IN (SELECT id FROM uploads WHERE state='deleted' AND deleted_at<?)",
	} {
		if _, err := tx.ExecContext(ctx, query, cutoff); err != nil {
			return err
		}
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, "DELETE FROM uploads WHERE id=? AND state='deleted'", id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delete(ctx, id)
}

func (s *Service) delete(ctx context.Context, id string) error {
	var state string
	if err := s.catalog.db.QueryRowContext(ctx, "SELECT state FROM uploads WHERE id=?", id).Scan(&state); err != nil {
		return err
	}
	if state == "deleted" {
		return nil
	}
	if _, err := s.catalog.db.ExecContext(ctx, "UPDATE uploads SET state='deletion_pending' WHERE id=? AND state!='deleted'", id); err != nil {
		return err
	}
	marker := []byte(strconv.FormatInt(s.now().Unix(), 10))
	version, err := s.storage.Put(ctx, id+"/deleted", marker)
	if err != nil && !errors.Is(err, ErrConflict) {
		return err
	}
	if _, err := s.storage.Get(ctx, id+"/deleted", version, 128); err != nil {
		return err
	}
	rows, err := s.catalog.db.QueryContext(ctx, "SELECT id,storage_version FROM objects WHERE upload_id=?", id)
	if err != nil {
		return err
	}
	defer rows.Close()
	type stored struct{ id, version string }
	objects := []stored{}
	for rows.Next() {
		var obj stored
		if err := rows.Scan(&obj.id, &obj.version); err != nil {
			return errors.Join(err, rows.Close())
		}
		objects = append(objects, obj)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return err
	}
	for _, obj := range objects {
		if err := s.storage.Delete(ctx, id+"/"+obj.id, obj.version); err != nil {
			return err
		}
		if _, err := s.storage.Get(ctx, id+"/"+obj.id, obj.version, 0); !errors.Is(err, ErrMissing) {
			if err == nil {
				return ErrStorage
			}
			return err
		}
	}
	tx, err := s.catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, query := range []string{
		"DELETE FROM outbox WHERE manifest_id IN (SELECT id FROM manifests WHERE upload_id=?)",
		"DELETE FROM manifests WHERE upload_id=?",
		"DELETE FROM objects WHERE upload_id=?",
	} {
		if _, err := tx.ExecContext(ctx, query, id); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE uploads SET state='deleted',reserved_bytes=0,retained_bytes=0,deleted_at=coalesce(deleted_at,?) WHERE id=?", s.now().Unix(), id); err != nil {
		return err
	}
	return tx.Commit()
}
