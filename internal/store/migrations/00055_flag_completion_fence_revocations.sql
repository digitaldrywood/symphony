-- +goose Up
UPDATE work_attempts
SET worker_metadata_json = json_set(
      CASE WHEN json_valid(worker_metadata_json) THEN worker_metadata_json ELSE '{}' END,
      '$.historical_completion_fence',
      json_object(
        'schema', 1,
        'migration', 55,
        'reason', 'completion_fence_unavailable',
        'excluded_from_worker_outcomes', json('true'),
        'original_worker_metadata_json', worker_metadata_json
      )
    )
WHERE status = 'terminal'
  AND completed_at IS NOT NULL
  AND (
    lower(trim(COALESCE(error_message, ''))) = 'completion_fence_unavailable'
    OR lower(trim(COALESCE(json_extract(
      CASE WHEN json_valid(worker_metadata_json) THEN worker_metadata_json ELSE '{}' END,
      '$.lane_revocation.reason'
    ), ''))) = 'completion_fence_unavailable'
    OR lower(trim(COALESCE(json_extract(
      CASE WHEN json_valid(worker_metadata_json) THEN worker_metadata_json ELSE '{}' END,
      '$.historical_lane_revocation.original_error_message'
    ), ''))) = 'completion_fence_unavailable'
    OR lower(trim(COALESCE(json_extract(
      CASE WHEN json_valid(worker_metadata_json) THEN worker_metadata_json ELSE '{}' END,
      '$.lane_revocation_receipt_backfill.original_error_message'
    ), ''))) = 'completion_fence_unavailable'
  );

-- +goose Down
UPDATE work_attempts
SET worker_metadata_json = json_extract(worker_metadata_json, '$.historical_completion_fence.original_worker_metadata_json')
WHERE json_extract(
  CASE WHEN json_valid(worker_metadata_json) THEN worker_metadata_json ELSE '{}' END,
  '$.historical_completion_fence.migration'
) = 55;
