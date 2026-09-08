package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/digitaldrywood/detent/internal/connector"
	"github.com/digitaldrywood/detent/internal/workspace"
)

func TestSessionBrakeProbeInitialization(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name               string
		probeErr, storeErr error
	}{
		{name: "initial read failure", probeErr: errors.New("unavailable")},
		{name: "initial journal failure", storeErr: errors.New("unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			controller := newSessionBrakeController(ctx, time.Time{}, 0, 0, time.Minute, cancel,
				func(context.Context) (sessionProgressSnapshot, error) { return sessionProgressSnapshot{}, tt.probeErr },
				func() time.Time { return at }, newControlledSessionTickerFactory().New,
				slog.New(slog.NewTextHandler(io.Discard, nil)), connector.Issue{},
				&sessionProgressJournal{store: &progressJournalStore{readErr: tt.storeErr}, sessionID: 1},
			)
			defer controller.Stop()
			controller.checkProgress(ctx, at.Add(time.Minute))
			if !errors.Is(context.Cause(ctx), ErrSessionNoProgress) {
				t.Fatalf("cause=%v", context.Cause(ctx))
			}
		})
	}
	if got := newSessionBrakeController(t.Context(), at, 0, 0, 0, nil, nil, nil, nil, nil, connector.Issue{}); got != nil {
		t.Fatal("disabled brake created")
	}
	controller := newSessionBrakeController(t.Context(), at, time.Minute, 0, 0, nil, nil, nil, nil, nil, connector.Issue{})
	if controller == nil || controller.watchDone != nil {
		t.Fatal("duration-only controller starts progress watch")
	}
}

func TestLocalProgressCannotExtendAbsoluteBounds(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		limit error
	}{
		{name: "duration", limit: ErrSessionDurationExceeded},
		{name: "turn count", limit: ErrSessionTurnLimitExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			before := sessionProgressSnapshot{LocalProgress: &workspace.LocalProgress{HeadSHA: "old", CommitFingerprint: "old"}}
			observation := sessionProgressObservation(before, at)
			controller := &sessionBrakeController{
				startedAt: at, lastProgressAt: at, maxTurns: 1, observation: &observation, cancelSession: cancel,
				now: func() time.Time { return at.Add(time.Minute) },
				probe: func(context.Context) (sessionProgressSnapshot, error) {
					return sessionProgressSnapshot{LocalProgress: &workspace.LocalProgress{HeadSHA: "new", CommitFingerprint: "new"}}, nil
				},
			}
			var err error
			if errors.Is(tt.limit, ErrSessionDurationExceeded) {
				err = controller.wrapDuration(ctx, tt.limit, time.Minute)
			} else {
				err = controller.observe(ctx, 2, 100)
			}
			if !errors.Is(err, tt.limit) || !controller.lastProgressAt.Equal(at.Add(time.Minute)) {
				t.Fatalf("bound=%v, progress=%v", err, controller.lastProgressAt)
			}
			if !errors.Is(controller.observe(ctx, 3, 200), tt.limit) {
				t.Fatal("breached controller resumed")
			}
		})
	}
}

func TestSessionProgressSnapshotReadFailures(t *testing.T) {
	t.Parallel()
	failed := errors.New("read failed")
	tests := []struct {
		name    string
		backend workspace.Backend
		wantErr bool
	}{
		{name: "recovery failure", backend: &fakeWorkspaceBackend{recoveryErr: failed}, wantErr: true},
		{name: "diff fallback failure", backend: diffOnlyProgressBackend{&fakeWorkspaceBackend{diffErr: failed}}, wantErr: true},
		{name: "diff fallback", backend: diffOnlyProgressBackend{&fakeWorkspaceBackend{diffStat: workspace.DiffStat{Files: 1, Fingerprint: "tracked"}}}},
		{name: "local failure", backend: failingLocalProgressBackend{&fakeWorkspaceBackend{}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := &Runner{}
			_, err := runner.sessionProgressSnapshot(t.Context(), tt.backend, workspace.Info{}, workspace.Issue{}, nil)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error=%v, want error %t", err, tt.wantErr)
			}
		})
	}
}

func TestSessionBrakeErrorBoundaries(t *testing.T) {
	t.Parallel()
	var empty *SessionBrakeError
	if empty.Error() == "" || empty.Unwrap() != nil || empty.Is(ErrSessionNoProgress) || sessionBrakeFingerprint(empty) != "" {
		t.Fatal("invalid nil error behavior")
	}
	if (&SessionBrakeError{}).Error() == "" {
		t.Fatal("missing error description")
	}
	at := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	controller := &sessionBrakeController{startedAt: at, lastProgressAt: at}
	brake := controller.memoryCeiling(&SessionMemoryCeilingError{RSSBytes: 200, CeilingBytes: 100}, at.Add(-time.Second))
	if brake.Elapsed != 0 || brake.RSSBytes != 200 || !brake.Is(ErrSessionMemoryCeilingExceeded) {
		t.Fatalf("memory brake=%+v", brake)
	}
	if controller.memoryCeiling(nil, at) != nil {
		t.Fatal("nil memory cause accepted")
	}
	if !(&SessionBrakeError{cause: io.EOF}).Is(io.EOF) {
		t.Fatal("wrapped cause lost")
	}
	if sessionProgressCheckInterval(time.Hour) != 5*time.Minute {
		t.Fatal("progress interval not bounded")
	}
	controller.probe = func(context.Context) (sessionProgressSnapshot, error) { return sessionProgressSnapshot{}, io.EOF }
	controller.refreshSnapshot(t.Context())
	if !controller.lastProgressAt.Equal(at) {
		t.Fatal("failed refresh advanced clock")
	}
	controller.journal = &sessionProgressJournal{sessionID: 1, store: &progressJournalStore{readErr: io.EOF}}
	controller.probe = func(context.Context) (sessionProgressSnapshot, error) { return sessionProgressSnapshot{}, nil }
	controller.now = func() time.Time { return at }
	controller.refreshSnapshot(t.Context())
	if !controller.lastProgressAt.Equal(at) {
		t.Fatal("failed journal refresh advanced clock")
	}
}

type diffOnlyProgressBackend struct{ workspace.Backend }

type failingLocalProgressBackend struct{ *fakeWorkspaceBackend }

func (failingLocalProgressBackend) LocalProgress(context.Context, workspace.Info, workspace.Issue) (workspace.LocalProgress, error) {
	return workspace.LocalProgress{}, errors.New("local progress unavailable")
}
