package runner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/digitaldrywood/detent/internal/config"
	"github.com/digitaldrywood/detent/internal/procgroup"
	"github.com/digitaldrywood/detent/internal/workspace"
)

var errPeriodicCheckpoint = errors.New("periodic worker checkpoint requested")

const checkpointTimeout = 2 * time.Minute

type workerCheckpoint struct {
	backend         workspace.CheckpointBackend
	plan            workspace.CheckpointPlan
	request         RunRequest
	lastFingerprint string
	prepareFailed   bool
	fingerprint     func(context.Context) (string, error)
	record          *workspace.CheckpointRecord
}

func (r *Runner) prepareWorkerCheckpoint(ctx context.Context, req RunRequest, backend workspace.Backend, info workspace.Info, issue workspace.Issue) *workerCheckpoint {
	checkpointBackend, ok := backend.(workspace.CheckpointBackend)
	if !ok || normalizeRunMode(req.Mode) != RunModeImplement {
		return nil
	}
	prepareCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	plan, err := checkpointBackend.PrepareCheckpoint(prepareCtx, info, issue)
	checkpoint := &workerCheckpoint{backend: checkpointBackend, plan: plan, request: req}
	if reader, ok := backend.(workspace.RecoveryStateProvider); ok {
		checkpoint.fingerprint = func(ctx context.Context) (string, error) {
			state, err := reader.RecoveryState(ctx, info, issue)
			return state.WorkspaceFingerprint, err
		}
	}
	if err != nil {
		r.logWorkerEvent(req.Issue, "worker_checkpoint_unavailable", "error", err)
		if plan.Journal != "" {
			checkpoint.prepareFailed = true
			r.saveWorkerCheckpoint(checkpoint, workspace.CheckpointRecord{Reason: "session_started", Status: "local_only", Detail: "Checkpoint preparation failed; workspace retained and publication unavailable."})
			return checkpoint
		}
		return nil
	}
	return checkpoint
}

func (c *workerCheckpoint) validate(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.request.Execution != nil {
		return c.request.Execution.Validate(ctx)
	}
	if c.request.CheckpointValidate == nil {
		return ErrExecutionAuthorityUnavailable
	}
	return c.request.CheckpointValidate(ctx)
}

func (r *Runner) saveWorkerCheckpoint(c *workerCheckpoint, record workspace.CheckpointRecord) {
	record.Schema = 1
	record.WorkspacePath = c.plan.Info.Path
	record.Branch = c.plan.Info.Branch
	c.record = &record
	if err := workspace.WriteCheckpointRecord(c.plan, record); err != nil {
		r.logWorkerEvent(c.request.Issue, "worker_checkpoint_journal_failed", "error", err)
	}
	r.logWorkerEvent(c.request.Issue, "worker_checkpoint", "status", record.Status, "reason", record.Reason, "head_sha", record.HeadSHA, "published", record.Published, "detail", record.Detail)
}

func (r *Runner) workerCheckpoint(ctx context.Context, c *workerCheckpoint, reason string, backend AgentBackend, turn AgentTurnRequest, run func(context.Context, AgentTurnRequest, RunRequest) agentTurnExecution) agentTurnExecution {
	checkpointCtx, cancel := context.WithTimeout(ctx, checkpointTimeout)
	defer cancel()
	if c.fingerprint != nil {
		fingerprint, err := c.fingerprint(checkpointCtx)
		delivered := c.record != nil && (c.record.Published || c.record.Status == "unchanged")
		if err == nil && fingerprint != "" && fingerprint == c.lastFingerprint && (reason == "periodic" || delivered) {
			return agentTurnExecution{}
		}
		defer func() {
			if fingerprint, err := c.fingerprint(checkpointCtx); err == nil {
				c.lastFingerprint = fingerprint
			}
		}()
	}
	record := workspace.CheckpointRecord{Reason: reason, Status: "pending", Detail: "Checkpoint requested; worker handshake and publication have not completed. Retain the local workspace."}
	r.saveWorkerCheckpoint(c, record)
	finish := func(detail string) {
		record.Status = "local_only"
		record.Detail = detail
		r.saveWorkerCheckpoint(c, record)
	}
	if c.prepareFailed {
		finish("Checkpoint preparation was unavailable. No checkpoint epilogue ran; inspect retained local work and its recovery journal.")
		return agentTurnExecution{}
	}
	if err := c.validate(checkpointCtx); err != nil {
		finish("Checkpoint skipped: cancellation or ownership could not be verified. No checkpoint epilogue ran; inspect retained local work.")
		return agentTurnExecution{}
	}
	if _, ok := backend.(AgentToolBackend); !ok {
		finish("Provider does not support the checkpoint handshake. No checkpoint epilogue ran; inspect retained local work.")
		return agentTurnExecution{}
	}
	turn.ReadOnly = true
	turn.SupplementalTools = true
	turn.MaxTurns = 2
	turn.MaxDuration = checkpointTimeout
	turn.ToolInstructions = "Inspect only. Submit the checkpoint selection once through detent_checkpoint_selection and return. Do not edit files, stage, commit, push, create a PR, or change tracker state."
	turn.Prompt = fmt.Sprintf("Detent requests a bounded recovery checkpoint for %s (%s). Review the retained worktree and ALL commits after base %s. Submit the exact HEAD and a complete list of intended issue file paths safe to publish, including paths in existing local commits. Exclude credentials, unrelated changes and scratch files. Set reviewed=true only after reviewing their contents and history. This is incomplete work; do not claim validated delivery or completion. If unsafe or uncertain, return without submitting.\n\n%s", c.request.Issue.Identifier, reason, c.plan.BaseSHA, turn.ToolInstructions)
	var selection workspace.CheckpointSelection
	var submitted bool
	var mu sync.Mutex
	req := c.request
	req.sessionBrake = nil
	req.AgentTools = []AgentTool{{Name: "detent_checkpoint_selection", Description: "Approve exact intended issue files and commit history for an incomplete recovery checkpoint. This selection authorizes no completion or review transition.", InputSchema: json.RawMessage(`{"type":"object","additionalProperties":false,"required":["head_sha","paths","reviewed"],"properties":{"head_sha":{"type":"string","minLength":40,"maxLength":64},"paths":{"type":"array","maxItems":1000,"items":{"type":"string"}},"reviewed":{"type":"boolean"}}}`)}}
	req.AgentToolHandler = func(ctx context.Context, call AgentToolCall) (AgentToolResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if call.Name != "detent_checkpoint_selection" || submitted {
			return AgentToolResult{Content: "checkpoint selection already submitted or unsupported tool"}, nil
		}
		if err := c.validate(ctx); err != nil {
			return AgentToolResult{}, err
		}
		decoder := json.NewDecoder(strings.NewReader(string(call.Arguments)))
		decoder.DisallowUnknownFields()
		var proposed workspace.CheckpointSelection
		if err := decoder.Decode(&proposed); err != nil {
			return AgentToolResult{Content: "invalid checkpoint selection"}, nil
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) || !proposed.Reviewed || len(proposed.Paths) > 1000 || len(proposed.HeadSHA) < 40 || len(proposed.HeadSHA) > 64 {
			return AgentToolResult{Content: "one reviewed selection with an exact HEAD is required"}, nil
		}
		selection = proposed
		submitted = true
		return AgentToolResult{Success: true, Content: "Selection received. Return now; Detent will verify and checkpoint after the worker process exits."}, nil
	}
	execution := run(checkpointCtx, turn, req)
	if execution.err != nil {
		finish("Checkpoint worker did not finish successfully. Publication was not attempted; inspect retained local work.")
		return execution
	}
	if !submitted {
		finish("Checkpoint worker returned without an approved selection. Publication was not attempted; inspect retained local work.")
		return execution
	}
	record.HeadSHA = selection.HeadSHA
	record.Paths = selection.Paths
	tempDir, err := workspace.PrepareWorkerScratch(checkpointCtx, c.plan.Info.Path)
	if err != nil {
		finish("Checkpoint credential isolation was unavailable. Publication was not attempted; inspect retained local work.")
		return execution
	}
	defer func() {
		if err := workspace.CleanupWorkerScratch(c.plan.Info.Path); err != nil {
			r.logWorkerEvent(c.request.Issue, "worker_checkpoint_scratch_cleanup_failed", "error", err)
		}
	}()
	turn.TempDir = tempDir
	if err := configureWorkerGitHubEnvironment(&turn); err != nil {
		finish("Checkpoint credentials were unavailable. Publication was not attempted; inspect retained local work.")
		return execution
	}
	for _, key := range []string{"TMPDIR", "TMP", "TEMP"} {
		turn.Environment.Variables[key] = tempDir
	}
	head, err := c.backend.Checkpoint(checkpointCtx, c.plan, selection, c.validate, turn.Environment)
	if head != "" {
		record.HeadSHA = head
	}
	if err != nil {
		r.logWorkerEvent(c.request.Issue, "worker_checkpoint_publish_failed", "error", err)
		finish("Checkpoint publication could not be verified (ownership, selected content, Git hooks, credentials, or remote state). Retain local work and inspect the remote before retrying.")
		return execution
	}
	record.Published = head != c.plan.BaseSHA
	record.Status = "unchanged"
	record.Detail = "No issue commit to publish. Checkpoint is not validated delivery."
	if record.Published {
		record.Status = "published"
		record.Detail = "Checkpoint commit verified on the owned remote branch. Work remains incomplete and subject to the normal completion, review, and CI gates."
		r.saveWorkerCheckpoint(c, record)
		url, err := checkpointPullRequest(checkpointCtx, c, turn, checkpointCommand)
		if err != nil {
			record.Detail += " Draft PR association unavailable; recover from the verified branch."
		} else {
			record.PullRequest = url
		}
	}
	r.saveWorkerCheckpoint(c, record)
	return execution
}

type checkpointCommander func(context.Context, AgentTurnRequest, ...string) (string, error)

func checkpointCommand(ctx context.Context, turn AgentTurnRequest, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "gh", args...)
	cmd.Dir = turn.Workspace
	cmd.WaitDelay = 5 * time.Second
	procgroup.SetEnvironment(cmd, turn.Environment)
	procgroup.SetTempDir(cmd, turn.TempDir)
	output, err := cmd.Output()
	return strings.TrimSpace(string(output)), err
}

func checkpointPullRequest(ctx context.Context, c *workerCheckpoint, turn AgentTurnRequest, command checkpointCommander) (string, error) {
	if err := c.validate(ctx); err != nil {
		return "", err
	}
	repository := strings.TrimSpace(turn.DeliverableRepository)
	if repository == "" {
		return "", errors.New("checkpoint PR repository is unavailable")
	}
	output, err := command(ctx, turn, "pr", "list", "--repo", repository, "--head", c.plan.Info.Branch, "--state", "open", "--json", "url", "--limit", "2")
	if err != nil {
		return "", err
	}
	var existing []struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal([]byte(output), &existing); err != nil {
		return "", err
	}
	if len(existing) == 1 {
		return existing[0].URL, nil
	}
	if len(existing) > 1 {
		return "", errors.New("multiple checkpoint pull requests require reconciliation")
	}
	if err := c.validate(ctx); err != nil {
		return "", err
	}
	return command(ctx, turn, "pr", "create", "--repo", repository, "--head", c.plan.Info.Branch, "--draft", "--title", "Checkpoint: "+c.request.Issue.Title, "--body", "Incomplete recovery checkpoint for "+c.request.Issue.Identifier+".\n\nWork is not validated delivery. The normal completion, review, and CI gates still apply.")
}

func (r *Runner) runCheckpointedTurn(parent, session context.Context, c *workerCheckpoint, cfg config.Agent, backend AgentBackend, turn AgentTurnRequest, req RunRequest, run func(context.Context, AgentTurnRequest, RunRequest) agentTurnExecution) agentTurnExecution {
	initialTokenOffset := req.sessionTokenOffset
	_, supportsCheckpoint := backend.(AgentToolBackend)
	if c != nil {
		c.request = req
	}
	var combined *agentTurnExecution
	for {
		turnCtx := session
		cancel := func() {}
		if c != nil && !c.prepareFailed && supportsCheckpoint && cfg.CheckpointIntervalMS > 0 {
			turnCtx, cancel = context.WithTimeoutCause(session, durationFromMillis(cfg.CheckpointIntervalMS), errPeriodicCheckpoint)
		}
		execution := run(turnCtx, turn, req)
		periodic := errors.Is(context.Cause(turnCtx), errPeriodicCheckpoint)
		cancel()
		if combined != nil {
			execution = mergeAgentTurnExecutions(*combined, execution)
		}
		if c == nil {
			return execution
		}
		if !periodic && !durationLimitError(execution.err) && parent.Err() == nil && execution.err == nil {
			execution.result.Checkpoint = c.record
			return execution
		}
		reason := "interrupted"
		if periodic {
			reason = "periodic"
		} else if errors.Is(execution.err, ErrSessionDurationExceeded) {
			reason = SessionBrakeReasonDuration
		} else if errors.Is(execution.err, ErrSessionNoProgress) {
			reason = SessionBrakeReasonNoProgress
		}
		if !periodic && !durationLimitError(execution.err) || errors.Is(execution.err, ErrWorkerProcessReap) || parent.Err() != nil {
			r.saveWorkerCheckpoint(c, workspace.CheckpointRecord{Reason: reason, Status: "local_only", Detail: "Worker interrupted or could not be reaped. No checkpoint epilogue ran; inspect the retained workspace and prior remote checkpoint evidence."})
			execution.result.Checkpoint = c.record
			return execution
		}
		checkpointTurn := turn
		checkpointTurn.Resume = AgentResume{ThreadID: execution.turnResult.ThreadID, SessionID: execution.turnResult.SessionID}
		c.request.sessionTurnOffset = execution.turnCount
		c.request.sessionTokenOffset = initialTokenOffset + execution.result.Tokens.TotalTokens
		recovery := r.workerCheckpoint(parent, c, reason, backend, checkpointTurn, run)
		originalErr, originalState := execution.err, execution.result.FinalState
		execution = mergeAgentTurnExecutions(execution, recovery)
		execution.err, execution.result.FinalState = originalErr, originalState
		execution.result.Checkpoint = c.record
		if periodic && session.Err() != nil {
			execution.err = context.Cause(session)
			execution.result.FinalState = finalStateForTurnError(execution.err)
		}
		if !periodic || session.Err() != nil || recovery.err != nil {
			return execution
		}
		turn.Resume = checkpointTurn.Resume
		if recovery.turnResult.ThreadID != "" || recovery.turnResult.SessionID != "" {
			turn.Resume = AgentResume{ThreadID: recovery.turnResult.ThreadID, SessionID: recovery.turnResult.SessionID}
		}
		turn.Prompt = "Continue the assigned issue work after the recovery checkpoint. Checkpoint publication is incomplete work; all original completion and review gates still apply."
		req.sessionTurnOffset = execution.turnCount
		req.sessionTokenOffset = initialTokenOffset + execution.result.Tokens.TotalTokens
		combined = &execution
	}
}
