CREATE TABLE IF NOT EXISTS knowledge_completion_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    knowledge_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    deleted_archived INTEGER NOT NULL DEFAULT 0,
    classified_legacy INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at DATETIME NULL,
    CONSTRAINT uq_knowledge_completion_attempt UNIQUE (knowledge_id, attempt)
);

CREATE INDEX IF NOT EXISTS idx_knowledge_completion_outbox_state
    ON knowledge_completion_outbox (state, id);
