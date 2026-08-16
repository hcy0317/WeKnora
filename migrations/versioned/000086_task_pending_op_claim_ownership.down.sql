DROP INDEX IF EXISTS idx_task_pending_ops_claim_heartbeat;

ALTER TABLE task_pending_ops
    DROP COLUMN IF EXISTS claim_heartbeat_at,
    DROP COLUMN IF EXISTS claimed_by_task_id,
    DROP COLUMN IF EXISTS claim_token;
