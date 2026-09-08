package artifact

import (
	"context"
	"errors"
	"net/http"
	"time"
)

type hostedStorage struct {
	service *Service
	storage Storage
}

func (m hostedStorage) Put(ctx context.Context, key string, data []byte) (string, error) {
	if err := m.service.recordTraffic(ctx, int64(len(data)), 0, 1); err != nil {
		return "", err
	}
	return m.storage.Put(ctx, key, data)
}

func (m hostedStorage) Get(ctx context.Context, key, version string, limit int64) ([]byte, error) {
	data, err := m.storage.Get(ctx, key, version, limit)
	return data, errors.Join(err, m.service.recordTraffic(ctx, 0, int64(len(data)), 1))
}

func (m hostedStorage) Delete(ctx context.Context, key, version string) error {
	err := m.storage.Delete(ctx, key, version)
	return errors.Join(err, m.service.recordTraffic(ctx, 0, 0, 1))
}

func (s *Service) recordTraffic(ctx context.Context, upload, download, requests int64) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	tx, err := s.catalog.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.now().Unix()

	if _, err := tx.ExecContext(ctx, "DELETE FROM hosted_traffic WHERE minute < ?", now-s.trafficRetention.Load()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_traffic(minute,upload_bytes,download_bytes,requests) VALUES(?,?,?,?) ON CONFLICT(minute) DO UPDATE SET upload_bytes=upload_bytes+excluded.upload_bytes,download_bytes=download_bytes+excluded.download_bytes,requests=requests+excluded.requests`, now/60*60, upload, download, requests); err != nil {
		return err
	}
	return tx.Commit()
}

func (h *RemoteHub) Limits(ctx context.Context, organization string) (Limits, error) {
	if organization != h.OrganizationID || h.PublisherToken == nil || !ValidID(h.ServiceID, "service") || !ValidID(organization, "org") {
		return Limits{}, ErrDenied
	}
	var usage Usage
	if h.Usage != nil {
		var err error
		usage, err = h.Usage(ctx)
		if err != nil {
			return Limits{}, err
		}
	}
	var limits Limits
	target := h.Origin + "/api/v2/organizations/" + organization + "/artifact-allowances/" + h.ServiceID
	err := h.requestResult(ctx, h.PublisherToken(), target, usage, &limits)
	return limits, err
}

func (s *Service) ReportHostedUsage(ctx context.Context) error {
	if s.config.Mode != "hosted" {
		return nil
	}
	limits, err := s.allowances.Limits(ctx, s.config.OrganizationID)
	if err != nil {
		return err
	}
	return s.configureTraffic(ctx, limits)
}

func hostedAllowanceStatus(status int) error {
	if status == http.StatusTooManyRequests {
		return ErrQuota
	}
	return ErrAuthorization
}

func (s *Service) configureTraffic(ctx context.Context, limits Limits) error {
	if limits.WindowSeconds < 60 || limits.WindowSeconds > 86400 || limits.WindowSeconds%60 != 0 || limits.TelemetrySeconds < limits.WindowSeconds || limits.TelemetrySeconds > 30*86400 || limits.RelayBytes < 0 {
		return ErrQuota
	}
	if s.trafficWindow.Load() == limits.WindowSeconds && s.trafficRetention.Load() == limits.TelemetrySeconds {
		return nil
	}
	if _, err := s.catalog.db.ExecContext(ctx, "UPDATE hosted_traffic_settings SET window_seconds=?,retention_seconds=? WHERE singleton=1", limits.WindowSeconds, limits.TelemetrySeconds); err != nil {
		return err
	}
	s.trafficWindow.Store(limits.WindowSeconds)
	s.trafficRetention.Store(limits.TelemetrySeconds)
	return nil
}
