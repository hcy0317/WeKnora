CREATE TABLE IF NOT EXISTS knowledge_completion_outbox (
    id BIGSERIAL PRIMARY KEY,
    knowledge_id VARCHAR(64) NOT NULL,
    attempt INT NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'pending',
    deleted_archived INT NOT NULL DEFAULT 0,
    classified_legacy INT NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ NULL,
    CONSTRAINT uq_knowledge_completion_attempt UNIQUE (knowledge_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_completion_outbox_state
    ON knowledge_completion_outbox (state, id);
