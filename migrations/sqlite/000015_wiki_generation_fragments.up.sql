CREATE TABLE IF NOT EXISTS wiki_generation_fragments (
    fragment_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    work_revision TEXT NOT NULL,
    purpose TEXT NOT NULL,
    fragment_key TEXT NOT NULL,
    prompt_digest TEXT NOT NULL,
    model_snapshot TEXT NOT NULL,
    state TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    call_id TEXT NOT NULL DEFAULT '',
    lease_until DATETIME NULL,
    output TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_wiki_generation_fragment_identity UNIQUE (
        tenant_id, knowledge_base_id, work_revision, purpose,
        fragment_key, prompt_digest, model_snapshot
    )
);
CREATE INDEX IF NOT EXISTS idx_wiki_generation_fragments_work
    ON wiki_generation_fragments (work_revision);
CREATE INDEX IF NOT EXISTS idx_wiki_generation_fragments_state
    ON wiki_generation_fragments (state);
