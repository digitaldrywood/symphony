-- name: CreateDetentRun :one
INSERT INTO detent_runs (
  started_at,
  stopped_at,
  restart_reason,
  peak_concurrent_agents,
  sessions_launched,
  input_tokens,
  output_tokens,
  total_tokens,
  runtime_seconds
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetDetentRun :one
SELECT *
FROM detent_runs
WHERE id = ?;

-- name: UpdateDetentRun :execrows
UPDATE detent_runs
SET stopped_at = COALESCE(?, stopped_at),
    restart_reason = COALESCE(?, restart_reason),
    peak_concurrent_agents = ?,
    sessions_launched = ?,
    input_tokens = ?,
    output_tokens = ?,
    total_tokens = ?,
    runtime_seconds = ?
WHERE id = ?;

-- name: CreateCodexSession :one
INSERT INTO codex_sessions (
  run_id,
  project_id,
  issue_id,
  identifier,
  issue_url,
  started_at,
  requested_model,
  agent_backend_id,
  agent_backend_kind,
  agent_role,
  work_attempt_id,
  agent_route,
  provider,
  provider_provenance,
  requested_model_provenance,
  model_provenance,
  reasoning_effort,
  reasoning_effort_provenance,
  service_tier,
  service_tier_provenance,
  identity_observed_at,
  completed_at,
  turns,
  input_tokens,
  cached_input_tokens,
  output_tokens,
  reasoning_output_tokens,
  total_tokens,
  model_context_window,
  runtime_seconds,
  final_state,
  model,
  provider_thread_id,
  provider_session_id,
  resumed_from_session_id,
  orphan_recovery_outcome,
  orphan_recovery_fallback_reason,
  runtime_identity_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: BackfillSessionProjectID :execrows
UPDATE codex_sessions
SET project_id = sqlc.arg(project_id)
WHERE trim(COALESCE(project_id, '')) = ''
  AND (
    lower(substr(identifier, 1, instr(identifier, '#') - 1)) = lower(sqlc.arg(repository))
    OR lower(substr(issue_url, 1, length('https://github.com/' || sqlc.arg(repository) || '/issues/'))) = lower('https://github.com/' || sqlc.arg(repository) || '/issues/')
  );

-- name: GetCodexSession :one
SELECT *
FROM codex_sessions
WHERE id = ?;

-- name: FinishCodexSession :execrows
UPDATE codex_sessions
SET completed_at = sqlc.arg(completed_at),
    turns = sqlc.arg(turns),
    input_tokens = sqlc.arg(input_tokens),
    cached_input_tokens = sqlc.narg(cached_input_tokens),
    output_tokens = sqlc.arg(output_tokens),
    reasoning_output_tokens = sqlc.narg(reasoning_output_tokens),
    total_tokens = sqlc.arg(total_tokens),
    model_context_window = COALESCE(sqlc.narg(model_context_window), model_context_window),
    runtime_seconds = sqlc.arg(runtime_seconds),
    final_state = sqlc.narg(final_state),
    model = COALESCE(sqlc.narg(model), model),
    provider_thread_id = COALESCE(sqlc.narg(provider_thread_id), provider_thread_id),
    provider_session_id = COALESCE(sqlc.narg(provider_session_id), provider_session_id),
    resumed_from_session_id = COALESCE(sqlc.narg(resumed_from_session_id), resumed_from_session_id),
    skill_draft_proposed = sqlc.arg(skill_draft_proposed)
WHERE id = sqlc.arg(id);

-- name: UpdateCodexSessionFinalStateByWorkAttempt :exec
UPDATE codex_sessions
SET final_state = sqlc.arg(final_state)
WHERE work_attempt_id = sqlc.arg(work_attempt_id);

-- name: UpdateCodexSessionIdentity :execrows
UPDATE codex_sessions
SET agent_backend_id = COALESCE(sqlc.narg(agent_backend_id), agent_backend_id),
    agent_backend_kind = COALESCE(sqlc.narg(agent_backend_kind), agent_backend_kind),
    agent_role = COALESCE(sqlc.narg(agent_role), agent_role),
    agent_route = COALESCE(sqlc.narg(agent_route), agent_route),
    provider = sqlc.narg(provider),
    provider_provenance = sqlc.narg(provider_provenance),
    requested_model = COALESCE(sqlc.narg(requested_model), requested_model),
    requested_model_provenance = sqlc.narg(requested_model_provenance),
    model = COALESCE(sqlc.narg(model), model),
    model_provenance = sqlc.narg(model_provenance),
    reasoning_effort = sqlc.narg(reasoning_effort),
    reasoning_effort_provenance = sqlc.narg(reasoning_effort_provenance),
    service_tier = sqlc.narg(service_tier),
    service_tier_provenance = sqlc.narg(service_tier_provenance),
    identity_observed_at = sqlc.narg(identity_observed_at),
    runtime_identity_json = sqlc.narg(runtime_identity_json)
WHERE id = sqlc.arg(id);

-- name: UpdateCodexSessionProviderIdentity :execrows
UPDATE codex_sessions
SET provider_thread_id = COALESCE(sqlc.narg(provider_thread_id), provider_thread_id),
    provider_session_id = COALESCE(sqlc.narg(provider_session_id), provider_session_id)
WHERE id = sqlc.arg(id);

-- name: UpdateCodexSessionWorkerProcess :execrows
UPDATE codex_sessions
SET worker_pid = sqlc.arg(worker_pid),
    worker_pgid = sqlc.arg(worker_pgid),
    worker_started_at = sqlc.arg(worker_started_at),
    worker_cleanup_root = sqlc.narg(worker_cleanup_root),
    worker_cleanup_path = sqlc.narg(worker_cleanup_path),
    worker_reaped_at = NULL,
    worker_reap_outcome = NULL,
    worker_reap_reason = NULL
WHERE id = sqlc.arg(id);

-- name: ListActiveWorkerProcesses :many
SELECT
  id AS session_id,
  CAST(COALESCE(issue_id, '') AS TEXT) AS issue_id,
  CAST(COALESCE(identifier, '') AS TEXT) AS identifier,
  CAST(COALESCE(issue_url, '') AS TEXT) AS issue_url,
  CAST(worker_pid AS INTEGER) AS worker_pid,
  CAST(COALESCE(worker_pgid, 0) AS INTEGER) AS worker_pgid,
  CAST(worker_started_at AS TEXT) AS worker_started_at,
  CAST(COALESCE(worker_cleanup_root, '') AS TEXT) AS worker_cleanup_root,
  CAST(COALESCE(worker_cleanup_path, '') AS TEXT) AS worker_cleanup_path,
  CAST(COALESCE(final_state, '') AS TEXT) AS final_state,
  CAST(COALESCE(completed_at, '') AS TEXT) AS completed_at
FROM codex_sessions
WHERE worker_reaped_at IS NULL
  AND worker_pid > 0
ORDER BY started_at, id;

-- name: MarkCodexSessionWorkerProcessReaped :execrows
UPDATE codex_sessions
SET worker_reaped_at = sqlc.arg(worker_reaped_at),
    worker_reap_outcome = sqlc.arg(worker_reap_outcome),
    worker_reap_reason = sqlc.arg(worker_reap_reason)
WHERE id = sqlc.arg(id);

-- name: UpdateCodexSessionResumeState :execrows
UPDATE codex_sessions
SET resumed_from_session_id = sqlc.narg(resumed_from_session_id),
    orphan_recovery_outcome = sqlc.narg(orphan_recovery_outcome),
    orphan_recovery_fallback_reason = sqlc.narg(orphan_recovery_fallback_reason),
    provider_thread_id = CASE WHEN sqlc.narg(resumed_from_session_id) IS NULL THEN NULL ELSE provider_thread_id END,
    provider_session_id = CASE WHEN sqlc.narg(resumed_from_session_id) IS NULL THEN NULL ELSE provider_session_id END
WHERE id = sqlc.arg(id);

-- name: ListOrphanedAgentSessions :many
SELECT
  s.id,
  s.work_attempt_id,
  w.project_id,
  CAST(COALESCE(s.issue_id, w.issue_id, '') AS TEXT) AS issue_id,
  CAST(COALESCE(s.identifier, w.identifier, '') AS TEXT) AS identifier,
  CAST(COALESCE(s.issue_url, w.issue_url, '') AS TEXT) AS issue_url,
  CAST(COALESCE(s.provider_thread_id, '') AS TEXT) AS provider_thread_id,
  CAST(COALESCE(s.provider_session_id, '') AS TEXT) AS provider_session_id,
  CAST(COALESCE(s.requested_model, '') AS TEXT) AS requested_model,
  CAST(COALESCE(s.model, '') AS TEXT) AS model,
  CAST(COALESCE(s.agent_backend_id, '') AS TEXT) AS agent_backend_id,
  CAST(COALESCE(s.agent_backend_kind, '') AS TEXT) AS agent_backend_kind,
  CAST(COALESCE(s.agent_role, '') AS TEXT) AS agent_role,
  CAST(COALESCE(s.runtime_identity_json, '') AS TEXT) AS runtime_identity_json,
  CAST(COALESCE(w.worker_type, '') AS TEXT) AS worker_type,
  CAST(COALESCE(w.worker_host, '') AS TEXT) AS worker_host,
  CAST(COALESCE(w.lane, '') AS TEXT) AS lane,
  w.attempt_number,
  CAST(s.started_at AS TEXT) AS started_at
FROM codex_sessions s
JOIN work_attempts w ON w.id = s.work_attempt_id
WHERE w.project_id = sqlc.arg(project_id)
  AND lower(trim(COALESCE(w.status, ''))) = 'active'
  AND s.completed_at IS NULL
  AND lower(trim(COALESCE(s.final_state, ''))) = 'running'
  AND (COALESCE(s.provider_thread_id, '') != '' OR COALESCE(s.provider_session_id, '') != '')
ORDER BY s.started_at DESC, s.id DESC;

-- name: MarkCodexSessionOrphaned :execrows
UPDATE codex_sessions
SET completed_at = sqlc.arg(completed_at),
    final_state = 'orphaned'
WHERE id = sqlc.arg(id)
  AND completed_at IS NULL
  AND lower(trim(COALESCE(final_state, ''))) = 'running';

-- name: GetLatestCompletedAgentResumeState :one
SELECT
  s.id,
  CAST(COALESCE(s.provider_thread_id, '') AS TEXT) AS provider_thread_id,
  CAST(COALESCE(s.provider_session_id, '') AS TEXT) AS provider_session_id,
  CAST(COALESCE(s.requested_model, '') AS TEXT) AS requested_model,
  CAST(COALESCE(s.model, '') AS TEXT) AS model,
  CAST(COALESCE(s.agent_backend_id, '') AS TEXT) AS agent_backend_id,
  CAST(COALESCE(s.agent_backend_kind, '') AS TEXT) AS agent_backend_kind,
  CAST(COALESCE(s.agent_role, '') AS TEXT) AS agent_role,
  CAST(COALESCE(s.runtime_identity_json, '') AS TEXT) AS runtime_identity_json,
  CAST(s.completed_at AS TEXT) AS completed_at
FROM codex_sessions AS s
JOIN work_attempts AS w ON w.id = s.work_attempt_id
WHERE s.completed_at IS NOT NULL
  AND w.completed_at IS NOT NULL
  AND lower(trim(COALESCE(s.final_state, ''))) = 'completed'
  AND (COALESCE(s.provider_thread_id, '') != '' OR COALESCE(s.provider_session_id, '') != '')
  AND COALESCE(s.project_id, '') = sqlc.arg(project_id)
  AND COALESCE(
    CASE WHEN json_valid(w.worker_metadata_json)
      THEN CAST(json_extract(w.worker_metadata_json, '$.pr_number') AS INTEGER)
      ELSE NULL
    END,
    w.pr_number,
    0
  ) = CAST(sqlc.arg(pr_number) AS INTEGER)
  AND CASE WHEN json_valid(w.worker_metadata_json)
    THEN CAST(COALESCE(json_extract(w.worker_metadata_json, '$.pr_head_sha'), '') AS TEXT)
    ELSE ''
  END = sqlc.arg(pr_head_sha)
  AND CASE WHEN json_valid(w.worker_metadata_json)
    THEN CAST(COALESCE(json_extract(w.worker_metadata_json, '$.pr_base_sha'), '') AS TEXT)
    ELSE ''
  END = sqlc.arg(pr_base_sha)
  AND COALESCE(s.agent_backend_id, '') = sqlc.arg(agent_backend_id)
  AND COALESCE(s.agent_backend_kind, '') = sqlc.arg(agent_backend_kind)
  AND COALESCE(s.agent_role, '') = sqlc.arg(agent_role)
  AND COALESCE(NULLIF(s.requested_model, ''), COALESCE(s.model, '')) = sqlc.arg(requested_model)
  AND (
    (sqlc.arg(issue_id) != '' AND COALESCE(s.issue_id, '') = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND COALESCE(s.identifier, '') = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND COALESCE(s.issue_url, '') = sqlc.arg(issue_url))
  )
ORDER BY s.completed_at DESC, s.id DESC
LIMIT 1;

-- name: GetLatestIssueAgentResumeState :one
SELECT
  id,
  CAST(COALESCE(project_id, '') AS TEXT) AS project_id,
  CAST(COALESCE(provider_thread_id, '') AS TEXT) AS provider_thread_id,
  CAST(COALESCE(provider_session_id, '') AS TEXT) AS provider_session_id,
  CAST(COALESCE(requested_model, '') AS TEXT) AS requested_model,
  CAST(COALESCE(model, '') AS TEXT) AS model,
  CAST(COALESCE(agent_backend_id, '') AS TEXT) AS agent_backend_id,
  CAST(COALESCE(agent_backend_kind, '') AS TEXT) AS agent_backend_kind,
  CAST(COALESCE(agent_role, '') AS TEXT) AS agent_role,
  CAST(COALESCE(runtime_identity_json, '') AS TEXT) AS runtime_identity_json,
  CAST(completed_at AS TEXT) AS completed_at
FROM codex_sessions
WHERE completed_at IS NOT NULL
  AND lower(trim(COALESCE(final_state, ''))) = 'completed'
  AND COALESCE(project_id, '') = sqlc.arg(project_id)
  AND (COALESCE(provider_thread_id, '') != '' OR COALESCE(provider_session_id, '') != '')
  AND (
    (sqlc.arg(issue_id) != '' AND COALESCE(issue_id, '') = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND COALESCE(identifier, '') = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND COALESCE(issue_url, '') = sqlc.arg(issue_url))
  )
ORDER BY completed_at DESC, id DESC
LIMIT 1;

-- name: GetLatestIssueAgentSession :one
SELECT
  id,
  CAST(COALESCE(project_id, '') AS TEXT) AS project_id,
  CAST(COALESCE(provider_thread_id, '') AS TEXT) AS provider_thread_id,
  CAST(COALESCE(provider_session_id, '') AS TEXT) AS provider_session_id,
  CAST(COALESCE(agent_backend_kind, '') AS TEXT) AS agent_backend_kind,
  CAST(completed_at AS TEXT) AS completed_at
FROM codex_sessions
WHERE completed_at IS NOT NULL
  AND COALESCE(project_id, '') = sqlc.arg(project_id)
  AND (COALESCE(provider_thread_id, '') != '' OR COALESCE(provider_session_id, '') != '')
  AND (
    (sqlc.arg(issue_id) != '' AND COALESCE(issue_id, '') = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND COALESCE(identifier, '') = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND COALESCE(issue_url, '') = sqlc.arg(issue_url))
  )
ORDER BY completed_at DESC, id DESC
LIMIT 1;

-- name: CreateAPIKey :one
INSERT INTO api_keys (
  id,
  name,
  prefix_last4,
  key_hash,
  scopes,
  project_ids,
  created_at,
  expires_at,
  last_used_at,
  revoked_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetAPIKey :one
SELECT *
FROM api_keys
WHERE id = ?;

-- name: GetAPIKeyByHash :one
SELECT *
FROM api_keys
WHERE key_hash = ?;

-- name: ListAPIKeys :many
SELECT *
FROM api_keys
ORDER BY created_at DESC, id DESC;

-- name: CountActiveAPIKeys :one
SELECT COUNT(*)
FROM api_keys
WHERE revoked_at IS NULL
  AND (
    expires_at IS NULL
    OR expires_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
  );

-- name: UpdateAPIKeyLastUsed :execrows
UPDATE api_keys
SET last_used_at = sqlc.arg(last_used_at)
WHERE id = sqlc.arg(id)
  AND (
    last_used_at IS NULL
    OR last_used_at <= sqlc.arg(threshold)
  );

-- name: SetAPIKeyExpiresAt :execrows
UPDATE api_keys
SET expires_at = sqlc.arg(expires_at)
WHERE id = sqlc.arg(id);

-- name: RevokeAPIKey :execrows
UPDATE api_keys
SET revoked_at = sqlc.arg(revoked_at)
WHERE id = sqlc.arg(id)
  AND revoked_at IS NULL;

-- name: CreateAPIUsageLog :exec
INSERT INTO api_usage_logs (
  api_key_id,
  method,
  path,
  status_code,
  latency_ms,
  ip,
  user_agent,
  created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- name: CountAPIUsageLogsByKey :one
SELECT COUNT(*)
FROM api_usage_logs
WHERE api_key_id = ?;

-- name: ListAPIUsageLogsByKey :many
SELECT *
FROM api_usage_logs
WHERE api_key_id = ?
ORDER BY created_at DESC, id DESC;

-- name: ListRecentCodexSessions :many
SELECT *
FROM codex_sessions
ORDER BY completed_at DESC, id DESC
LIMIT ?;

-- name: ListIssueCodexSessions :many
SELECT *
FROM codex_sessions
WHERE project_id = sqlc.arg(project_id)
  AND (
    (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
  )
ORDER BY started_at, id;

-- name: CompletedIssueCycleRows :many
WITH issue_sessions AS (
  SELECT
    COALESCE(NULLIF(identifier, ''), NULLIF(issue_id, ''), NULLIF(issue_url, ''), 'unassigned') AS issue_key,
    started_at,
    completed_at,
    lower(trim(COALESCE(final_state, ''))) NOT IN ('failed', 'failure', 'cancelled', 'canceled', 'orphaned') AS successful
  FROM codex_sessions
  WHERE started_at IS NOT NULL
    AND completed_at IS NOT NULL
),
successful_issues AS (
  SELECT
    issue_key,
    MAX(completed_at) AS completed_at
  FROM issue_sessions
  WHERE successful
  GROUP BY issue_key
)
SELECT
  CAST(issue_sessions.issue_key AS TEXT) AS issue_key,
  CAST(MIN(issue_sessions.started_at) AS TEXT) AS started_at,
  CAST(successful_issues.completed_at AS TEXT) AS completed_at,
  CAST(COUNT(*) AS INTEGER) AS sessions
FROM issue_sessions
JOIN successful_issues ON successful_issues.issue_key = issue_sessions.issue_key
WHERE issue_sessions.started_at <= successful_issues.completed_at
GROUP BY issue_sessions.issue_key, successful_issues.completed_at
ORDER BY completed_at DESC, issue_key;

-- name: LifetimeTotals :one
SELECT
  CAST(COALESCE(SUM(input_tokens), 0) AS INTEGER) AS input_tokens,
  CAST(COALESCE(SUM(cached_input_tokens), 0) AS INTEGER) AS cached_input_tokens,
  CAST(COALESCE(SUM(output_tokens), 0) AS INTEGER) AS output_tokens,
  CAST(COALESCE(SUM(reasoning_output_tokens), 0) AS INTEGER) AS reasoning_output_tokens,
  CAST(COALESCE(SUM(total_tokens), 0) AS INTEGER) AS total_tokens,
  CAST(COALESCE(SUM(runtime_seconds), 0) AS INTEGER) AS runtime_seconds,
  CAST(COUNT(*) AS INTEGER) AS sessions,
  CAST((SELECT COUNT(*) FROM detent_runs) AS INTEGER) AS runs,
  CAST(COALESCE(SUM(CASE WHEN orphan_recovery_outcome = 'resumed' THEN 1 ELSE 0 END), 0) AS INTEGER) AS orphan_resumed,
  CAST(COALESCE(SUM(CASE WHEN orphan_recovery_outcome = 'fresh' THEN 1 ELSE 0 END), 0) AS INTEGER) AS orphan_fresh,
  CAST(COALESCE(SUM(CASE WHEN orphan_recovery_outcome = 'resumed' THEN input_tokens ELSE 0 END), 0) AS INTEGER) AS resumed_input_tokens,
  CAST(COALESCE(SUM(CASE WHEN orphan_recovery_outcome = 'resumed' THEN cached_input_tokens ELSE 0 END), 0) AS INTEGER) AS resumed_cached_tokens
FROM codex_sessions
WHERE completed_at IS NOT NULL;

-- name: DailyTokenSpend :many
SELECT
  CAST(COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '') AS TEXT) AS model,
  CAST(COALESCE(SUM(input_tokens), 0) AS INTEGER) AS input_tokens,
  CAST(COALESCE(SUM(cached_input_tokens), 0) AS INTEGER) AS cached_input_tokens,
  CAST(COALESCE(SUM(output_tokens), 0) AS INTEGER) AS output_tokens,
  CAST(COALESCE(SUM(reasoning_output_tokens), 0) AS INTEGER) AS reasoning_output_tokens,
  CAST(COALESCE(SUM(total_tokens), 0) AS INTEGER) AS total_tokens,
  CAST(COUNT(*) AS INTEGER) AS sessions
FROM codex_sessions
WHERE substr(completed_at, 1, 10) = ?
GROUP BY COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '')
ORDER BY COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '');

-- name: ProjectDailyTokenSpend :many
SELECT
  CAST(COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '') AS TEXT) AS model,
  CAST(COALESCE(SUM(input_tokens), 0) AS INTEGER) AS input_tokens,
  CAST(COALESCE(SUM(cached_input_tokens), 0) AS INTEGER) AS cached_input_tokens,
  CAST(COALESCE(SUM(output_tokens), 0) AS INTEGER) AS output_tokens,
  CAST(COALESCE(SUM(reasoning_output_tokens), 0) AS INTEGER) AS reasoning_output_tokens,
  CAST(COALESCE(SUM(total_tokens), 0) AS INTEGER) AS total_tokens,
  CAST(COUNT(*) AS INTEGER) AS sessions
FROM codex_sessions
WHERE substr(completed_at, 1, 10) = sqlc.arg(completed_at)
  AND (
    project_id = sqlc.arg(project_id)
    OR trim(COALESCE(project_id, '')) = ''
  )
GROUP BY COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '')
ORDER BY COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '');

-- name: IssueTokenSpend :many
SELECT
  CAST(COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '') AS TEXT) AS model,
  CAST(COALESCE(SUM(input_tokens), 0) AS INTEGER) AS input_tokens,
  CAST(COALESCE(SUM(cached_input_tokens), 0) AS INTEGER) AS cached_input_tokens,
  CAST(COALESCE(SUM(output_tokens), 0) AS INTEGER) AS output_tokens,
  CAST(COALESCE(SUM(reasoning_output_tokens), 0) AS INTEGER) AS reasoning_output_tokens,
  CAST(COALESCE(SUM(total_tokens), 0) AS INTEGER) AS total_tokens,
  CAST(COUNT(*) AS INTEGER) AS sessions
FROM codex_sessions
WHERE COALESCE(project_id, '') = sqlc.arg(project_id)
  AND (
    issue_id = sqlc.arg(issue_id)
    OR identifier = sqlc.arg(identifier)
    OR issue_url = sqlc.arg(issue_url)
  )
GROUP BY COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '')
ORDER BY COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), '');

-- name: RecentModelTokenQuantiles :one
WITH recent AS (
  SELECT
    input_tokens,
    COALESCE(cached_input_tokens, 0) AS cached_input_tokens,
    output_tokens,
    total_tokens
  FROM codex_sessions
  WHERE completed_at IS NOT NULL
    AND lower(trim(COALESCE(NULLIF(model, ''), NULLIF(requested_model, ''), ''))) = lower(trim(sqlc.arg(model)))
  ORDER BY completed_at DESC, id DESC
  LIMIT sqlc.arg(limit)
),
counts AS (
  SELECT
    CAST(COUNT(*) AS INTEGER) AS sessions
  FROM recent
),
targets AS (
  SELECT
    sessions,
    CAST((sessions + 1) / 2 AS INTEGER) AS p50_rank,
    CAST(((sessions * 9) + 9) / 10 AS INTEGER) AS p90_rank
  FROM counts
),
ranked AS (
  SELECT
    input_tokens,
    cached_input_tokens,
    output_tokens,
    total_tokens,
    ROW_NUMBER() OVER (ORDER BY input_tokens) AS input_rank,
    ROW_NUMBER() OVER (ORDER BY cached_input_tokens) AS cached_input_rank,
    ROW_NUMBER() OVER (ORDER BY output_tokens) AS output_rank,
    ROW_NUMBER() OVER (ORDER BY total_tokens) AS total_rank
  FROM recent
)
SELECT
  CAST(targets.sessions AS INTEGER) AS sessions,
  CAST(COALESCE((SELECT input_tokens FROM ranked WHERE input_rank = targets.p50_rank), 0) AS INTEGER) AS p50_input_tokens,
  CAST(COALESCE((SELECT input_tokens FROM ranked WHERE input_rank = targets.p90_rank), 0) AS INTEGER) AS p90_input_tokens,
  CAST(COALESCE((SELECT cached_input_tokens FROM ranked WHERE cached_input_rank = targets.p50_rank), 0) AS INTEGER) AS p50_cached_input_tokens,
  CAST(COALESCE((SELECT cached_input_tokens FROM ranked WHERE cached_input_rank = targets.p90_rank), 0) AS INTEGER) AS p90_cached_input_tokens,
  CAST(COALESCE((SELECT output_tokens FROM ranked WHERE output_rank = targets.p50_rank), 0) AS INTEGER) AS p50_output_tokens,
  CAST(COALESCE((SELECT output_tokens FROM ranked WHERE output_rank = targets.p90_rank), 0) AS INTEGER) AS p90_output_tokens,
  CAST(COALESCE((SELECT total_tokens FROM ranked WHERE total_rank = targets.p50_rank), 0) AS INTEGER) AS p50_total_tokens,
  CAST(COALESCE((SELECT total_tokens FROM ranked WHERE total_rank = targets.p90_rank), 0) AS INTEGER) AS p90_total_tokens
FROM targets;

-- name: CreateUsageEvent :one
INSERT INTO usage_events (
  project_id,
  run_id,
  session_id,
  issue_id,
  identifier,
  pr_number,
  model,
  input_tokens,
  cached_input_tokens,
  output_tokens,
  reasoning_output_tokens,
  total_tokens,
  model_context_window,
  cost_usd,
  projected_cost_usd,
  projection_overshoot_usd,
  runtime_seconds,
  started_at,
  finished_at,
  event_day,
  outcome
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: GetUsageEvent :one
SELECT *
FROM usage_events
WHERE id = ?;

-- name: UsageReportRows :many
WITH usage_report_rows AS (
  SELECT
    CASE
      WHEN sqlc.arg(bucket_by) = 'day' THEN event_day
      WHEN sqlc.arg(bucket_by) = 'project' THEN project_id
      WHEN sqlc.arg(bucket_by) = 'issue' THEN COALESCE(NULLIF(identifier, ''), NULLIF(issue_id, ''), 'unassigned')
      WHEN sqlc.arg(bucket_by) = 'pr' THEN project_id || '#' || COALESCE(CAST(pr_number AS TEXT), 'unassigned')
      WHEN sqlc.arg(bucket_by) = 'model' THEN COALESCE(NULLIF(model, ''), 'unassigned')
      ELSE event_day
    END AS group_key,
    COALESCE(NULLIF(model, ''), 'unassigned') AS model,
    input_tokens,
    cached_input_tokens,
    output_tokens,
    reasoning_output_tokens,
    total_tokens,
    model_context_window,
    runtime_seconds
  FROM usage_events
  WHERE (sqlc.narg(from_day) IS NULL OR event_day >= sqlc.narg(from_day))
    AND (sqlc.narg(to_day) IS NULL OR event_day <= sqlc.narg(to_day))
)
SELECT
  CAST(usage_report_rows.group_key AS TEXT) AS group_key,
  CAST(usage_report_rows.model AS TEXT) AS model,
  CAST(COALESCE(SUM(usage_report_rows.input_tokens), 0) AS INTEGER) AS input_tokens,
  CAST(COALESCE(SUM(usage_report_rows.cached_input_tokens), 0) AS INTEGER) AS cached_input_tokens,
  CAST(COALESCE(SUM(usage_report_rows.output_tokens), 0) AS INTEGER) AS output_tokens,
  CAST(COALESCE(SUM(usage_report_rows.reasoning_output_tokens), 0) AS INTEGER) AS reasoning_output_tokens,
  CAST(COALESCE(SUM(usage_report_rows.total_tokens), 0) AS INTEGER) AS total_tokens,
  CAST(COALESCE(MAX(usage_report_rows.model_context_window), 0) AS INTEGER) AS model_context_window,
  CAST(COALESCE(SUM(usage_report_rows.runtime_seconds), 0) AS INTEGER) AS runtime_seconds,
  CAST(COUNT(*) AS INTEGER) AS events
FROM usage_report_rows
GROUP BY usage_report_rows.group_key, usage_report_rows.model
ORDER BY usage_report_rows.group_key, usage_report_rows.model;

-- name: BudgetCostEvents :many
SELECT
  project_id,
  finished_at,
  cost_usd
FROM usage_events
WHERE finished_at >= sqlc.arg(from_time)
  AND finished_at < sqlc.arg(to_time)
ORDER BY finished_at, id;

-- name: IssueSpendSince :one
SELECT
  CAST(COALESCE(SUM(usage_events.cost_usd), 0) AS REAL) AS cost_usd,
  CAST(COALESCE(SUM(usage_events.total_tokens), 0) AS INTEGER) AS total_tokens,
  CAST(COUNT(*) AS INTEGER) AS sessions,
  CAST(COALESCE(MIN(usage_events.finished_at), '') AS TEXT) AS first_session_at,
  CAST(COALESCE(MAX(usage_events.finished_at), '') AS TEXT) AS last_session_at
FROM usage_events
LEFT JOIN codex_sessions AS session ON session.id = usage_events.session_id
LEFT JOIN work_attempts AS attempt ON attempt.id = session.work_attempt_id
WHERE usage_events.project_id = sqlc.arg(project_id)
  AND usage_events.started_at > sqlc.arg(since)
  AND lower(trim(COALESCE(attempt.terminal_state, ''))) != 'capacity'
  AND (
    (sqlc.arg(issue_id) != '' AND COALESCE(usage_events.issue_id, '') = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND COALESCE(usage_events.identifier, '') = sqlc.arg(identifier))
  );

-- name: ListFairShareUsage :many
SELECT
  project_id,
  weight,
  dispatches,
  runtime_seconds,
  updated_at
FROM fair_share_usage
ORDER BY project_id;

-- name: UpsertFairShareUsage :one
INSERT INTO fair_share_usage (
  project_id,
  weight,
  dispatches,
  runtime_seconds,
  updated_at
) VALUES (?, ?, 1, ?, ?)
ON CONFLICT(project_id) DO UPDATE SET
  weight = excluded.weight,
  dispatches = fair_share_usage.dispatches + excluded.dispatches,
  runtime_seconds = fair_share_usage.runtime_seconds + excluded.runtime_seconds,
  updated_at = excluded.updated_at
RETURNING *;

-- name: CreateWorkflowPhaseEvent :one
INSERT INTO workflow_phase_events (
  project_id,
  run_id,
  session_id,
  issue_id,
  identifier,
  issue_url,
  pr_number,
  phase_type,
  phase_name,
  previous_phase_name,
  reason,
  status,
  started_at,
  finished_at,
  duration_seconds,
  event_day,
  command_name,
  exit_code,
  turns,
  input_tokens,
  cached_input_tokens,
  output_tokens,
  reasoning_output_tokens,
  total_tokens,
  model_context_window,
  endpoint_family,
  metadata_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ProvenanceAttributionTrustBoundary :one
SELECT trustworthy_since
FROM provenance_attribution_boundaries
WHERE id = 1;

-- name: WorkflowPhaseDurationRows :many
SELECT *
FROM workflow_phase_events
WHERE finished_at IS NOT NULL
  AND (sqlc.narg(project_id) IS NULL OR project_id = sqlc.narg(project_id))
  AND (sqlc.narg(from_time) IS NULL OR finished_at >= sqlc.narg(from_time))
  AND (sqlc.narg(to_time) IS NULL OR finished_at < sqlc.narg(to_time))
ORDER BY project_id, phase_type, phase_name, finished_at, id;

-- name: WorkflowPhaseFlowRows :many
SELECT event.*
FROM workflow_phase_events AS event
WHERE event.finished_at IS NOT NULL
  AND event.phase_type IN ('agent_session', 'local_check', 'ci')
  AND (sqlc.narg(project_id) IS NULL OR event.project_id = sqlc.narg(project_id))
  AND (sqlc.narg(from_time) IS NULL OR event.finished_at > sqlc.narg(from_time))
  AND (sqlc.narg(to_time) IS NULL OR event.started_at < sqlc.narg(to_time))
ORDER BY event.project_id, event.phase_type, event.phase_name, event.finished_at, event.id;

-- name: IssueWorkflowTimelineRows :many
SELECT *
FROM workflow_phase_events
WHERE project_id = sqlc.arg(project_id)
  AND (
    issue_id = sqlc.arg(issue_id)
    OR identifier = sqlc.arg(identifier)
    OR issue_url = sqlc.arg(issue_url)
  )
ORDER BY started_at, id;

-- name: CreateWorkAttempt :one
INSERT INTO work_attempts (
  project_id,
  issue_id,
  identifier,
  issue_url,
  pr_number,
  repo,
  worker_type,
  worker_host,
  lane,
  attempt_number,
  status,
  started_at,
  lease_expires_at,
  heartbeat_at,
  phase,
  status_message,
  current_step,
  total_steps,
  progress_percent,
  current_command,
  wait_reason,
  github_rate_snapshot_json,
  ci_state,
  capacity_snapshot_json,
  worker_metadata_json,
  metrics_json,
  next_action,
  detent_session_id,
  provider_session_id,
  runtime_identity_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: CreateLaneMutationReceipt :one
INSERT INTO lane_mutation_receipts (
  project_id,
  issue_id,
  work_attempt_id,
  generation,
  disposition,
  from_state,
  to_state,
  reason,
  tracker_result,
  requested_at
)
SELECT
  sqlc.arg(project_id),
  sqlc.arg(issue_id),
  work_attempts.id,
  sqlc.arg(generation),
  sqlc.arg(disposition),
  sqlc.arg(from_state),
  sqlc.arg(to_state),
  sqlc.arg(reason),
  'prepared',
  sqlc.arg(requested_at)
FROM work_attempts
WHERE work_attempts.id = sqlc.arg(work_attempt_id)
  AND work_attempts.project_id = sqlc.arg(project_id)
  AND COALESCE(work_attempts.issue_id, '') = sqlc.arg(issue_id)
  AND work_attempts.status = 'active'
  AND work_attempts.completed_at IS NULL
RETURNING *;

-- name: SupersedeLaneMutationReceipts :execrows
UPDATE lane_mutation_receipts
SET tracker_result = 'superseded',
    resolved_at = sqlc.arg(superseded_at),
    error_message = NULL
WHERE project_id = sqlc.arg(project_id)
  AND issue_id = sqlc.arg(issue_id)
  AND work_attempt_id = sqlc.arg(work_attempt_id)
  AND generation = sqlc.arg(generation)
  AND id < sqlc.arg(newer_receipt_id)
  AND tracker_result IN ('prepared', 'applied')
  AND consumed_at IS NULL;

-- name: ResolveLaneMutationReceipt :execrows
UPDATE lane_mutation_receipts
SET tracker_result = sqlc.arg(tracker_result),
    resolved_at = sqlc.arg(resolved_at),
    error_message = NULLIF(sqlc.arg(error_message), '')
WHERE id = sqlc.arg(id)
  AND work_attempt_id = sqlc.arg(work_attempt_id)
  AND generation = sqlc.arg(generation)
  AND tracker_result = 'prepared'
  AND consumed_at IS NULL;

-- name: LaneMutationReceiptForOwner :one
SELECT *
FROM lane_mutation_receipts
WHERE project_id = sqlc.arg(project_id)
  AND issue_id = sqlc.arg(issue_id)
  AND work_attempt_id = sqlc.arg(work_attempt_id)
  AND generation = sqlc.arg(generation)
  AND lower(trim(to_state)) = lower(trim(sqlc.arg(to_state)))
  AND tracker_result IN ('prepared', 'applied')
  AND consumed_at IS NULL
ORDER BY id DESC
LIMIT 1;

-- name: ConsumeLaneMutationReceipt :one
UPDATE lane_mutation_receipts
SET consumed_at = sqlc.arg(consumed_at)
WHERE id = sqlc.arg(id)
  AND project_id = sqlc.arg(project_id)
  AND issue_id = sqlc.arg(issue_id)
  AND work_attempt_id = sqlc.arg(work_attempt_id)
  AND generation = sqlc.arg(generation)
  AND lower(trim(to_state)) = lower(trim(sqlc.arg(to_state)))
  AND tracker_result IN ('prepared', 'applied')
  AND consumed_at IS NULL
RETURNING *;

-- name: GetWorkAttempt :one
SELECT *
FROM work_attempts
WHERE id = ?;

-- name: UpdateWorkAttemptHeartbeat :execrows
UPDATE work_attempts
SET heartbeat_at = ?,
    lease_expires_at = ?,
    phase = ?,
    status_message = ?,
    current_step = ?,
    total_steps = ?,
    progress_percent = ?,
    current_command = ?,
    wait_reason = ?,
    github_rate_snapshot_json = ?,
    ci_state = ?,
    capacity_snapshot_json = ?,
    worker_metadata_json = ?,
    metrics_json = ?,
    next_action = ?,
    error_class = ?,
    error_message = ?,
    detent_session_id = COALESCE(sqlc.narg(detent_session_id), detent_session_id),
    provider_session_id = COALESCE(sqlc.narg(provider_session_id), provider_session_id),
    runtime_identity_json = COALESCE(NULLIF(sqlc.arg(runtime_identity_json), '{}'), runtime_identity_json)
WHERE id = sqlc.arg(work_attempt_id)
  AND completed_at IS NULL;

-- name: CompleteWorkAttempt :execrows
UPDATE work_attempts
SET status = ?,
    terminal_state = ?,
    completed_at = ?,
    heartbeat_at = ?,
    lease_expires_at = ?,
    error_class = ?,
    error_message = ?,
    phase = ?,
    status_message = ?,
    wait_reason = ?,
    github_rate_snapshot_json = ?,
    ci_state = ?,
    capacity_snapshot_json = ?,
    worker_metadata_json = ?,
    metrics_json = ?,
    next_action = ?,
    detent_session_id = COALESCE(sqlc.narg(detent_session_id), detent_session_id),
    provider_session_id = COALESCE(sqlc.narg(provider_session_id), provider_session_id),
    runtime_identity_json = COALESCE(NULLIF(sqlc.arg(runtime_identity_json), '{}'), runtime_identity_json)
WHERE id = sqlc.arg(work_attempt_id)
  AND completed_at IS NULL;

-- name: ListActiveWorkAttempts :many
SELECT *
FROM work_attempts
WHERE completed_at IS NULL
  AND (sqlc.arg(filter_project_id) = '' OR project_id = sqlc.arg(filter_project_id))
ORDER BY started_at, id;

-- name: ListRecentTerminalWorkAttempts :many
SELECT *
FROM work_attempts
WHERE completed_at IS NOT NULL
  AND status = 'terminal'
  AND (sqlc.arg(filter_project_id) = '' OR project_id = sqlc.arg(filter_project_id))
  AND (sqlc.arg(filter_worker_type) = '' OR worker_type = sqlc.arg(filter_worker_type))
  AND (
    (sqlc.arg(issue_id) = '' AND sqlc.arg(identifier) = '' AND sqlc.arg(issue_url) = '')
    OR
    (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
  )
ORDER BY completed_at DESC, id DESC
LIMIT sqlc.arg(result_limit);

-- name: ListIssueWorkAttempts :many
SELECT *
FROM work_attempts
WHERE project_id = sqlc.arg(project_id)
  AND (
    (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
  )
ORDER BY started_at, id;

-- name: ListPendingWorkAttemptCapacityReleases :many
SELECT *
FROM work_attempts
WHERE project_id = sqlc.arg(project_id)
  AND status = 'terminal'
  AND completed_at IS NOT NULL
  AND lower(trim(COALESCE(next_action, ''))) = 'release capacity'
ORDER BY completed_at, id;

-- name: ClearWorkAttemptCapacityRelease :exec
UPDATE work_attempts
SET next_action = NULL
WHERE id = sqlc.arg(work_attempt_id)
  AND status = 'terminal'
  AND completed_at IS NOT NULL
  AND lower(trim(COALESCE(next_action, ''))) = 'release capacity';

-- name: UpdateOperatorStop :execrows
UPDATE work_attempts
SET phase = sqlc.arg(phase),
    status_message = sqlc.arg(status_message),
    worker_metadata_json = sqlc.arg(worker_metadata_json),
    next_action = sqlc.arg(next_action)
WHERE id = sqlc.arg(work_attempt_id)
  AND terminal_state = 'operator_stopped';

-- name: ListPendingOperatorStops :many
SELECT *
FROM work_attempts
WHERE project_id = sqlc.arg(project_id)
  AND terminal_state = 'operator_stopped'
  AND phase IN ('operator_stop_pending', 'operator_stop_transition_failed')
ORDER BY completed_at, id;

-- name: TimeoutExpiredWorkAttempts :many
UPDATE work_attempts
SET status = ?,
    terminal_state = ?,
    completed_at = ?,
    heartbeat_at = ?,
    error_class = ?,
    error_message = ?,
    phase = ?,
    status_message = ?
WHERE completed_at IS NULL
  AND (sqlc.arg(filter_project_id) = '' OR project_id = sqlc.arg(filter_project_id))
  AND lease_expires_at IS NOT NULL
  AND lease_expires_at <= sqlc.arg(lease_expires_at)
  AND lower(trim(COALESCE(phase, ''))) != 'completion_deferred'
RETURNING *;

-- name: ReclaimActiveWorkAttempts :many
UPDATE work_attempts
SET status = ?,
    terminal_state = ?,
    completed_at = ?,
    heartbeat_at = ?,
    error_class = ?,
    error_message = ?,
    phase = ?,
    status_message = ?
WHERE completed_at IS NULL
  AND project_id = ?
  AND lower(trim(COALESCE(phase, ''))) != 'completion_deferred'
RETURNING *;

-- name: CreateSchedulerDecision :one
INSERT INTO scheduler_decisions (
  project_id,
  issue_id,
  identifier,
  issue_url,
  pr_number,
  repo,
  lane,
  queue_position,
  result,
  reason,
  selected,
  retry,
  attempt_number,
  worker_host,
  decision_at,
  wait_reason,
  capacity_snapshot_json,
  github_rate_snapshot_json,
  metadata_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
RETURNING *;

-- name: ListRecentSchedulerDecisions :many
SELECT *
FROM scheduler_decisions
WHERE sqlc.arg(filter_project_id) = '' OR project_id = sqlc.arg(filter_project_id)
ORDER BY decision_at DESC, id DESC
LIMIT sqlc.arg(limit);

-- name: ListIssueSchedulerDecisions :many
SELECT *
FROM scheduler_decisions
WHERE project_id = sqlc.arg(project_id)
  AND (
    (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
    OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
    OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
  )
ORDER BY decision_at DESC, id DESC
LIMIT sqlc.arg(limit);

-- name: UpsertProjectDispatchStatus :one
INSERT INTO project_dispatch_status (
  project_id,
  candidate_count,
  eligible_candidate_count,
  candidate_fingerprint,
  selected_count,
  skipped_count,
  wait_reason,
  wait_reason_code,
  all_skipped_since,
  last_selected_at,
  observed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id) DO UPDATE SET
  candidate_count = excluded.candidate_count,
  eligible_candidate_count = excluded.eligible_candidate_count,
  candidate_fingerprint = excluded.candidate_fingerprint,
  selected_count = excluded.selected_count,
  skipped_count = excluded.skipped_count,
  wait_reason = excluded.wait_reason,
  wait_reason_code = excluded.wait_reason_code,
  all_skipped_since = excluded.all_skipped_since,
  last_selected_at = excluded.last_selected_at,
  observed_at = excluded.observed_at
RETURNING *;

-- name: GetProjectDispatchStatus :one
SELECT *
FROM project_dispatch_status
WHERE project_id = ?;

-- name: ListHealthNotificationStates :many
SELECT *
FROM health_notification_states
ORDER BY identity;

-- name: UpsertHealthNotificationState :exec
INSERT INTO health_notification_states (
  identity,
  state_json,
  updated_at
) VALUES (?, ?, ?)
ON CONFLICT(identity) DO UPDATE SET
  state_json = excluded.state_json,
  updated_at = excluded.updated_at;

-- name: ListIssueActivityEvents :many
WITH issue_events AS (
  SELECT
    printf('scheduler:%d', id) AS event_id,
    'scheduler' AS source,
    'decision' AS kind,
    result AS name,
    CAST(decision_at AS TEXT) AS event_at,
    attempt_number,
    CAST(0 AS INTEGER) AS session_id,
    COALESCE(lane, '') AS detail,
    COALESCE(NULLIF(wait_reason, ''), reason, '') AS reason,
    result AS status,
    '' AS model,
    CAST(0 AS INTEGER) AS turns,
    CAST(0 AS INTEGER) AS total_tokens,
    CAST(0 AS INTEGER) AS verbose
  FROM scheduler_decisions
  WHERE (sqlc.arg(project_id) = '' OR project_id = sqlc.arg(project_id))
    AND (
      (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
      OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
    )

  UNION ALL

  SELECT
    printf('workflow:%d', id),
    'workflow',
    phase_type,
    phase_name,
    CAST(COALESCE(finished_at, started_at) AS TEXT),
    CAST(0 AS INTEGER),
    COALESCE(session_id, 0),
    COALESCE(previous_phase_name, ''),
    COALESCE(reason, ''),
    COALESCE(status, ''),
    '',
    turns,
    total_tokens,
    CASE WHEN phase_type = 'agent_session' AND total_tokens > 0 THEN 1 ELSE 0 END
  FROM workflow_phase_events
  WHERE (sqlc.arg(project_id) = '' OR project_id = sqlc.arg(project_id))
    AND (
      (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
      OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
    )

  UNION ALL

  SELECT
    printf('attempt:%d:start', id),
    'work_attempt',
    'attempt',
    'started',
    CAST(started_at AS TEXT),
    attempt_number,
    COALESCE(detent_session_id, 0),
    COALESCE(NULLIF(status_message, ''), NULLIF(current_command, ''), phase, ''),
    COALESCE(NULLIF(wait_reason, ''), error_message, ''),
    status,
    '',
    CAST(0 AS INTEGER),
    CAST(0 AS INTEGER),
    CAST(0 AS INTEGER)
  FROM work_attempts
  WHERE (sqlc.arg(project_id) = '' OR project_id = sqlc.arg(project_id))
    AND (
      (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
      OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
    )

  UNION ALL

  SELECT
    printf('attempt:%d:finish', id),
    'work_attempt',
    'attempt',
    'finished',
    CAST(completed_at AS TEXT),
    attempt_number,
    COALESCE(detent_session_id, 0),
    COALESCE(NULLIF(status_message, ''), phase, ''),
    COALESCE(NULLIF(wait_reason, ''), error_message, ''),
    COALESCE(terminal_state, status),
    '',
    CAST(0 AS INTEGER),
    CAST(0 AS INTEGER),
    CAST(0 AS INTEGER)
  FROM work_attempts
  WHERE completed_at IS NOT NULL
    AND (sqlc.arg(project_id) = '' OR project_id = sqlc.arg(project_id))
    AND (
      (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
      OR (sqlc.arg(issue_url) != '' AND issue_url = sqlc.arg(issue_url))
    )

  UNION ALL

  SELECT
    printf('session:%d:start', session.id),
    'session',
    'session',
    'started',
    CAST(session.started_at AS TEXT),
    COALESCE(attempt.attempt_number, 0),
    session.id,
    COALESCE(session.agent_backend_kind, ''),
    '',
    'started',
    COALESCE(session.model, ''),
    CAST(0 AS INTEGER),
    CAST(0 AS INTEGER),
    CAST(0 AS INTEGER)
  FROM codex_sessions AS session
  LEFT JOIN work_attempts AS attempt ON attempt.id = session.work_attempt_id
  WHERE session.started_at IS NOT NULL
    AND (sqlc.arg(project_id) = '' OR attempt.project_id = sqlc.arg(project_id) OR attempt.project_id IS NULL)
    AND (
      (sqlc.arg(issue_id) != '' AND session.issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND session.identifier = sqlc.arg(identifier))
      OR (sqlc.arg(issue_url) != '' AND session.issue_url = sqlc.arg(issue_url))
    )

  UNION ALL

  SELECT
    printf('session:%d:finish', session.id),
    'session',
    'session',
    'finished',
    CAST(session.completed_at AS TEXT),
    COALESCE(attempt.attempt_number, 0),
    session.id,
    COALESCE(session.agent_backend_kind, ''),
    '',
    COALESCE(session.final_state, 'completed'),
    COALESCE(session.model, ''),
    session.turns,
    session.total_tokens,
    CAST(0 AS INTEGER)
  FROM codex_sessions AS session
  LEFT JOIN work_attempts AS attempt ON attempt.id = session.work_attempt_id
  WHERE session.completed_at IS NOT NULL
    AND (sqlc.arg(project_id) = '' OR attempt.project_id = sqlc.arg(project_id) OR attempt.project_id IS NULL)
    AND (
      (sqlc.arg(issue_id) != '' AND session.issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND session.identifier = sqlc.arg(identifier))
      OR (sqlc.arg(issue_url) != '' AND session.issue_url = sqlc.arg(issue_url))
    )

  UNION ALL

  SELECT
    printf('usage:%d', id),
    'usage',
    'usage',
    'turn_usage',
    CAST(finished_at AS TEXT),
    CAST(0 AS INTEGER),
    COALESCE(session_id, 0),
    outcome,
    '',
    outcome,
    model,
    CAST(0 AS INTEGER),
    total_tokens,
    CAST(1 AS INTEGER)
  FROM usage_events
  WHERE (sqlc.arg(project_id) = '' OR project_id = sqlc.arg(project_id))
    AND (
      (sqlc.arg(issue_id) != '' AND issue_id = sqlc.arg(issue_id))
      OR (sqlc.arg(identifier) != '' AND identifier = sqlc.arg(identifier))
    )
)
SELECT
  event_id,
  source,
  kind,
  name,
  event_at,
  attempt_number,
  session_id,
  detail,
  reason,
  status,
  model,
  turns,
  total_tokens,
  verbose
FROM issue_events
WHERE sqlc.arg(include_verbose) = 1 OR verbose = 0
ORDER BY event_at DESC, event_id DESC
LIMIT sqlc.arg(limit) OFFSET sqlc.arg(offset);

-- name: UpsertValidatorVerdict :one
INSERT INTO validator_verdicts (
  project_id,
  issue_id,
  head_sha,
  identifier,
  issue_url,
  pr_number,
  submitted,
  verdict,
  score,
  summary,
  findings_json,
  commented,
  failure_attempts,
  next_retry_at,
  recorded_at,
  updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id, issue_id, head_sha) DO UPDATE SET
  identifier = excluded.identifier,
  issue_url = excluded.issue_url,
  pr_number = excluded.pr_number,
  submitted = excluded.submitted,
  verdict = excluded.verdict,
  score = excluded.score,
  summary = excluded.summary,
  findings_json = excluded.findings_json,
  commented = excluded.commented,
  failure_attempts = excluded.failure_attempts,
  next_retry_at = excluded.next_retry_at,
  recorded_at = excluded.recorded_at,
  updated_at = excluded.updated_at
RETURNING *;

-- name: GetValidatorVerdict :one
SELECT *
FROM validator_verdicts
WHERE project_id = ?
  AND issue_id = ?
  AND head_sha = ?;

-- name: ListValidatorVerdicts :many
SELECT *
FROM validator_verdicts
WHERE (sqlc.arg(filter_project_id) = '' OR project_id = sqlc.arg(filter_project_id))
  AND (sqlc.narg(from_time) IS NULL OR updated_at >= sqlc.narg(from_time))
  AND (sqlc.narg(to_time) IS NULL OR updated_at < sqlc.narg(to_time))
ORDER BY updated_at DESC, id DESC;

-- name: MarkValidatorVerdictCommented :execrows
UPDATE validator_verdicts
SET commented = 1,
    updated_at = ?
WHERE project_id = ?
  AND issue_id = ?
  AND head_sha = ?;

-- name: DailyDigestRuntime :one
SELECT
  (SELECT COUNT(*) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at)) AS sessions,
  (SELECT CAST(COALESCE(SUM(session.input_tokens), 0) AS INTEGER) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at)) AS input_tokens,
  (SELECT CAST(COALESCE(SUM(session.cached_input_tokens), 0) AS INTEGER) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at)) AS cached_input_tokens,
  (SELECT CAST(COALESCE(SUM(session.output_tokens), 0) AS INTEGER) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at)) AS output_tokens,
  (SELECT CAST(COALESCE(SUM(session.total_tokens), 0) AS INTEGER) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at)) AS total_tokens,
  (SELECT COUNT(*) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at) AND lower(trim(COALESCE(session.orphan_recovery_outcome, ''))) = 'resumed') AS orphan_resumed,
  (SELECT COUNT(*) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at) AND lower(trim(COALESCE(session.orphan_recovery_outcome, ''))) = 'fresh') AS orphan_fresh,
  (SELECT COUNT(*) FROM codex_sessions AS session WHERE session.completed_at >= sqlc.arg(from_at) AND session.completed_at < sqlc.arg(to_at) AND lower(trim(COALESCE(session.final_state, ''))) IN ('failed', 'failure', 'cancelled', 'canceled', 'orphaned', 'token_ceiling_exceeded')) AS failed_sessions,
  (SELECT COUNT(*) FROM work_attempts AS attempt WHERE attempt.started_at < sqlc.arg(to_at) AND attempt.completed_at > sqlc.arg(from_at) AND lower(trim(COALESCE(attempt.terminal_state, ''))) = 'capacity') AS capacity_outages,
  (SELECT CAST(COALESCE(SUM(MAX(0, CAST(strftime('%s', MIN(attempt.completed_at, sqlc.arg(to_at))) AS INTEGER) - CAST(strftime('%s', MAX(attempt.started_at, sqlc.arg(from_at))) AS INTEGER))), 0) AS INTEGER) FROM work_attempts AS attempt WHERE attempt.started_at < sqlc.arg(to_at) AND attempt.completed_at > sqlc.arg(from_at) AND lower(trim(COALESCE(attempt.terminal_state, ''))) = 'capacity') AS capacity_seconds,
  (SELECT COUNT(DISTINCT trip.identifier) FROM (
    SELECT COALESCE(NULLIF(decision.identifier, ''), printf('decision:%d', decision.id)) AS identifier
    FROM scheduler_decisions AS decision
    WHERE decision.decision_at >= sqlc.arg(from_at)
      AND decision.decision_at < sqlc.arg(to_at)
      AND (lower(COALESCE(decision.reason, '')) LIKE '%circuit_breaker%' OR lower(COALESCE(decision.wait_reason, '')) LIKE '%circuit_breaker%')
    UNION ALL
    SELECT COALESCE(NULLIF(event.identifier, ''), printf('event:%d', event.id)) AS identifier
    FROM workflow_phase_events AS event
    WHERE event.started_at >= sqlc.arg(from_at)
      AND event.started_at < sqlc.arg(to_at)
      AND lower(COALESCE(event.reason, '')) LIKE '%circuit_breaker%'
  ) AS trip) AS breaker_trips;

-- name: DailyDigestModels :many
SELECT
  CAST(COALESCE(NULLIF(trim(model), ''), NULLIF(trim(requested_model), ''), 'unassigned') AS TEXT) AS model,
  CAST(COALESCE(SUM(input_tokens), 0) AS INTEGER) AS input_tokens,
  CAST(COALESCE(SUM(cached_input_tokens), 0) AS INTEGER) AS cached_input_tokens,
  CAST(COALESCE(SUM(output_tokens), 0) AS INTEGER) AS output_tokens,
  CAST(COALESCE(SUM(reasoning_output_tokens), 0) AS INTEGER) AS reasoning_output_tokens,
  CAST(COALESCE(SUM(total_tokens), 0) AS INTEGER) AS total_tokens,
  COUNT(*) AS sessions
FROM codex_sessions
WHERE completed_at >= sqlc.arg(from_at)
  AND completed_at < sqlc.arg(to_at)
GROUP BY COALESCE(NULLIF(trim(model), ''), NULLIF(trim(requested_model), ''), 'unassigned')
ORDER BY model;

-- name: DailyDigestFailureClasses :many
SELECT
  CAST(COALESCE(NULLIF(trim(error_class), ''), 'unknown') AS TEXT) AS error_class,
  COUNT(*) AS failures
FROM work_attempts
WHERE completed_at >= sqlc.arg(from_at)
  AND completed_at < sqlc.arg(to_at)
  AND lower(trim(COALESCE(terminal_state, ''))) IN ('failure', 'timed_out', 'no_progress', 'capacity')
GROUP BY COALESCE(NULLIF(trim(error_class), ''), 'unknown')
ORDER BY failures DESC, error_class
LIMIT 1;

-- name: DailyDigestCapacityModes :many
SELECT
  CAST(COALESCE(NULLIF(trim(next_action), ''), NULLIF(trim(wait_reason), ''), 'automatic retry') AS TEXT) AS recovery_mode,
  COUNT(*) AS outages
FROM work_attempts
WHERE started_at < sqlc.arg(to_at)
  AND completed_at > sqlc.arg(from_at)
  AND lower(trim(COALESCE(terminal_state, ''))) = 'capacity'
GROUP BY COALESCE(NULLIF(trim(next_action), ''), NULLIF(trim(wait_reason), ''), 'automatic retry')
ORDER BY outages DESC, recovery_mode
LIMIT 1;

-- name: UpsertBudgetOverride :one
INSERT INTO budget_overrides (
  project_id,
  per_day_max_usd,
  per_issue_max_usd,
  expires_at,
  created_at,
  reason
) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT(project_id) DO UPDATE SET
  per_day_max_usd = excluded.per_day_max_usd,
  per_issue_max_usd = excluded.per_issue_max_usd,
  expires_at = excluded.expires_at,
  created_at = excluded.created_at,
  reason = excluded.reason
RETURNING *;

-- name: ActiveBudgetOverride :one
SELECT *
FROM budget_overrides
WHERE project_id = sqlc.arg(project_id)
  AND expires_at > sqlc.arg(now);

-- name: ListActiveBudgetOverrides :many
SELECT *
FROM budget_overrides
WHERE expires_at > sqlc.arg(now)
ORDER BY expires_at, project_id;

-- name: DeleteBudgetOverride :execrows
DELETE FROM budget_overrides
WHERE project_id = ?;

-- name: CreateMagicLink :exec
INSERT INTO auth_magic_links (
  token_hash,
  email,
  expires_at,
  created_at
) VALUES (
  sqlc.arg(token_hash),
  sqlc.arg(email),
  sqlc.arg(expires_at),
  sqlc.arg(created_at)
);

-- name: ConsumeMagicLink :one
UPDATE auth_magic_links
SET used_at = sqlc.arg(now)
WHERE token_hash = sqlc.arg(token_hash)
  AND used_at IS NULL
  AND expires_at > sqlc.arg(now)
RETURNING email;

-- name: CreateWebSession :exec
INSERT INTO auth_sessions (
  token_hash,
  email,
  expires_at,
  created_at
) VALUES (
  sqlc.arg(token_hash),
  sqlc.arg(email),
  sqlc.arg(expires_at),
  sqlc.arg(created_at)
);

-- name: GetWebSession :one
SELECT email, expires_at
FROM auth_sessions
WHERE token_hash = sqlc.arg(token_hash)
  AND expires_at > sqlc.arg(now);

-- name: CreateSecurityAuditRun :one
INSERT INTO security_audit_runs (
  invocation_id,
  project_id,
  issue_id,
  identifier,
  issue_url,
  repository,
  pr_number,
  base_sha,
  head_sha,
  service_identity,
  reviewer_version,
  reviewer_digest,
  authentication_mode,
  worker_pid,
  worker_pgid,
  worker_started_at,
  provider_thread_id,
  provider_session_id,
  exit_status,
  failure,
  output_digest,
  output_bytes,
  verdict,
  summary,
  findings_json,
  attempt,
  started_at,
  completed_at,
  recorded_at
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
RETURNING id;

-- name: LatestSecurityAuditRun :one
SELECT *
FROM security_audit_runs
WHERE project_id = sqlc.arg(project_id)
  AND repository = sqlc.arg(repository)
  AND pr_number = sqlc.arg(pr_number)
  AND base_sha = sqlc.arg(base_sha)
  AND head_sha = sqlc.arg(head_sha)
ORDER BY recorded_at DESC, id DESC
LIMIT 1;

-- name: LatestSecurityAuditRunForPullRequest :one
SELECT *
FROM security_audit_runs
WHERE project_id = sqlc.arg(project_id)
  AND repository = sqlc.arg(repository)
  AND pr_number = sqlc.arg(pr_number)
ORDER BY recorded_at DESC, id DESC
LIMIT 1;

-- name: CreateSecurityAuditDisposition :one
INSERT INTO security_audit_dispositions (
  audit_run_id,
  finding_id,
  status,
  evidence,
  service_identity,
  recorded_at
) VALUES (?, ?, ?, ?, ?, ?)
RETURNING id;

-- name: ListSecurityAuditDispositions :many
SELECT *
FROM security_audit_dispositions
WHERE audit_run_id = ?
ORDER BY recorded_at, id;
