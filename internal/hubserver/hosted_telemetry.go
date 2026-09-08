package hubserver

import (
	"context"
	"errors"
	"time"

	"github.com/labstack/echo/v4"
)

func (s *Service) recordHostedRequest(c echo.Context, started time.Time) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request().Context()), 2*time.Second)
	defer cancel()
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		s.config.Logger.Warn("hosted request metrics unavailable")
		return
	}
	defer tx.Rollback()
	now := s.config.now()
	for _, sample := range []struct {
		metric string
		amount int64
	}{
		{"http_requests", 1},
		{"http_duration_microseconds", max(int64(0), time.Since(started).Microseconds())},
		{"http_response_bytes", max(int64(0), c.Response().Size)},
	} {
		if err := s.database.recordHostedUsage(ctx, tx, now, sample.metric, sample.amount); err != nil {
			s.config.Logger.Warn("hosted request metrics unavailable")
			return
		}
	}
	if err := tx.Commit(); err != nil {
		s.config.Logger.Warn("hosted request metrics unavailable")
	}
}

func (s *Service) reserveHostedInvitation(ctx context.Context, email string) error {
	tx, err := s.database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := s.config.now()
	before, err := s.database.hostedConsumption(ctx, tx, now)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM hosted_member_reservations WHERE expires_at <= ?", now.Unix()); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO hosted_member_reservations(email,expires_at) SELECT ?,? WHERE NOT EXISTS(SELECT 1 FROM hosted_members WHERE email = ? AND active = 1) ON CONFLICT(email) DO UPDATE SET expires_at = excluded.expires_at`, email, now.Add(time.Duration(s.database.hostedPlans.InvitationSeconds)*time.Second).Unix(), email); err != nil {
		return err
	}
	if err := s.database.checkHostedGrowth(ctx, tx, before, now, false); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Service) releaseHostedInvitation(ctx context.Context, email string) error {
	_, err := s.database.db.ExecContext(ctx, "DELETE FROM hosted_member_reservations WHERE email = ?", email)
	return err
}

func (s *Service) hostedInvitationFailure(c echo.Context, email string, cause error) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request().Context()), 2*time.Second)
	defer cancel()
	err := errors.Join(cause, s.releaseHostedInvitation(ctx, email))
	if err != nil {
		s.config.Logger.Warn("hosted invitation failed")
	}
	return s.hostedError(c, 503, "The invitation could not be sent")
}
