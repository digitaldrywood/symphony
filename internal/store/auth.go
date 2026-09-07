package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/digitaldrywood/detent/internal/auth"
	"github.com/digitaldrywood/detent/internal/store/sqlc"
)

func (s *sqliteStore) CreateMagicLink(ctx context.Context, link auth.MagicLink) error {
	if err := s.queries.CreateMagicLink(ctx, sqlc.CreateMagicLinkParams{
		TokenHash: link.TokenHash,
		Email:     link.Email,
		ExpiresAt: link.ExpiresAt,
		CreatedAt: link.CreatedAt,
	}); err != nil {
		return fmt.Errorf("create magic link: %w", err)
	}
	return nil
}

func (s *sqliteStore) ConsumeMagicLink(ctx context.Context, consumption auth.MagicLinkConsumption) (session auth.Session, err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return session, fmt.Errorf("begin magic link transaction: %w", err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, tx.Rollback())
		}
	}()
	queries := s.queries.WithTx(tx)
	email, err := queries.ConsumeMagicLink(ctx, sqlc.ConsumeMagicLinkParams{
		Now:       sql.NullTime{Time: consumption.Now, Valid: true},
		TokenHash: consumption.TokenHash,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return session, auth.ErrInvalidLink
	}
	if err != nil {
		return session, fmt.Errorf("consume magic link: %w", err)
	}
	if err := queries.CreateWebSession(ctx, sqlc.CreateWebSessionParams{
		TokenHash: consumption.SessionHash,
		Email:     email,
		ExpiresAt: consumption.SessionExpiresAt,
		CreatedAt: consumption.Now,
	}); err != nil {
		return session, fmt.Errorf("create web session from magic link: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return session, fmt.Errorf("commit magic link transaction: %w", err)
	}
	return auth.Session{Email: email, ExpiresAt: consumption.SessionExpiresAt}, nil
}

func (s *sqliteStore) CreateWebSession(ctx context.Context, session auth.SessionRecord) error {
	if session.Identity != nil {
		return auth.ErrInvalidSession
	}
	if err := s.queries.CreateWebSession(ctx, sqlc.CreateWebSessionParams{
		TokenHash: session.TokenHash,
		Email:     session.Email,
		ExpiresAt: session.ExpiresAt,
		CreatedAt: session.CreatedAt,
	}); err != nil {
		return fmt.Errorf("create web session: %w", err)
	}
	return nil
}

func (s *sqliteStore) WebSession(ctx context.Context, tokenHash string, now time.Time) (auth.Session, error) {
	row, err := s.queries.GetWebSession(ctx, sqlc.GetWebSessionParams{TokenHash: tokenHash, Now: now})
	if errors.Is(err, sql.ErrNoRows) {
		return auth.Session{}, auth.ErrInvalidSession
	}
	if err != nil {
		return auth.Session{}, fmt.Errorf("get web session: %w", err)
	}
	return auth.Session{Email: row.Email, ExpiresAt: row.ExpiresAt}, nil
}
