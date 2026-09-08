package runner

import (
	"context"
	"errors"
	"time"

	"github.com/digitaldrywood/detent/internal/store"
)

type sessionProgressJournal struct {
	store     store.SessionProgressStore
	sessionID int64
	resumeID  int64
	loaded    bool
	saved     bool
}

func (r *Runner) sessionProgressJournal(sessionID, resumeID int64) *sessionProgressJournal {
	progressStore, ok := r.store.(store.SessionProgressStore)
	if !ok || sessionID <= 0 {
		return nil
	}
	return &sessionProgressJournal{store: progressStore, sessionID: sessionID, resumeID: resumeID}
}

func (j *sessionProgressJournal) load(ctx context.Context) (store.SessionProgress, error) {
	progress, err := j.store.SessionProgress(ctx, j.sessionID)
	if err == nil {
		j.saved = true
	}
	if errors.Is(err, store.ErrNotFound) && j.resumeID > 0 {
		return j.store.SessionProgress(ctx, j.resumeID)
	}
	return progress, err
}

func sessionProgressObservation(snapshot sessionProgressSnapshot, at time.Time) store.SessionProgress {
	observation := store.SessionProgress{
		HeadSHA: snapshot.HeadSHA, WorkpadFingerprint: snapshot.WorkpadFingerprint,
		WorkspaceFingerprint: snapshot.fingerprint(), LastProgressAt: at,
	}
	if local := snapshot.LocalProgress; local != nil {
		observation.Local = true
		observation.HeadSHA = local.HeadSHA
		observation.CommitFingerprint = local.CommitFingerprint
		observation.TrackedFingerprint = local.TrackedFingerprint
		observation.WorkspaceFingerprint = ""
	}
	return observation
}

func sessionProgressAdvanced(previous, current store.SessionProgress) bool {
	if previous.Local != current.Local {
		return false
	}
	if previous.WorkpadFingerprint != current.WorkpadFingerprint {
		return true
	}
	if current.Local {
		return (current.CommitFingerprint != "" && current.CommitFingerprint != previous.CommitFingerprint) ||
			(current.TrackedFingerprint != "" && current.TrackedFingerprint != previous.TrackedFingerprint)
	}
	return previous.WorkspaceFingerprint != current.WorkspaceFingerprint
}

func (c *sessionBrakeController) observeSnapshotLocked(ctx context.Context, snapshot sessionProgressSnapshot, at time.Time) error {
	if c.journal != nil && !c.journal.loaded {
		previous, err := c.journal.load(ctx)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return err
		}
		if err == nil {
			c.observation = &previous
			c.lastProgressAt = previous.LastProgressAt
		}
		c.journal.loaded = true
	}
	next := sessionProgressObservation(snapshot, c.lastProgressAt)
	advanced := c.observation != nil && sessionProgressAdvanced(*c.observation, next)
	if advanced && at.After(c.lastProgressAt) {
		next.LastProgressAt = at
	}
	if c.journal != nil && (!c.journal.saved || c.observation == nil || *c.observation != next) {
		if err := c.journal.store.SaveSessionProgress(ctx, c.journal.sessionID, next); err != nil {
			return err
		}
		c.journal.saved = true
	}
	c.observation = &next
	c.lastProgressAt = next.LastProgressAt
	c.workProductProgress = c.workProductProgress || advanced
	return nil
}
