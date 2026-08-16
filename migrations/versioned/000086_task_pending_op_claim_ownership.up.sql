ALTER TABLE task_pending_ops
    ADD COLUMN IF NOT EXISTS claim_token VARCHAR(64),
    ADD COLUMN IF NOT EXISTS claimed_by_task_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS claim_heartbeat_at TIMESTAMPTZ;

COMMENT ON COLUMN task_pending_ops.claim_token IS
    'Stable fencing token for one claim term. Renewals update heartbeat only; successor claims replace the token.';
COMMENT ON COLUMN task_pending_ops.claimed_by_task_id IS
    'Concrete Asynq task id that owns the current claim term.';
COMMENT ON COLUMN task_pending_ops.claim_heartbeat_at IS
    'Last successful owner heartbeat. Stale recovery falls back to claimed_at for pre-migration rows.';

CREATE INDEX IF NOT EXISTS idx_task_pending_ops_claim_heartbeat
    ON task_pending_ops (task_type, scope, scope_id, claim_heartbeat_at)
    WHERE claimed_at IS NOT NULL;
