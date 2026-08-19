CREATE TABLE IF NOT EXISTS wiki_generation_fragments (
    fragment_id VARCHAR(64) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    work_revision VARCHAR(64) NOT NULL,
    purpose VARCHAR(64) NOT NULL,
    fragment_key VARCHAR(64) NOT NULL,
    prompt_digest VARCHAR(64) NOT NULL,
    model_snapshot VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    attempts INT NOT NULL DEFAULT 0,
    call_id VARCHAR(36) NOT NULL DEFAULT '',
    lease_until TIMESTAMPTZ NULL,
    output TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_wiki_generation_fragment_identity UNIQUE (
        tenant_id, knowledge_base_id, work_revision, purpose,
        fragment_key, prompt_digest, model_snapshot
    )
);

CREATE INDEX IF NOT EXISTS idx_wiki_generation_fragments_work
    ON wiki_generation_fragments (work_revision);
CREATE INDEX IF NOT EXISTS idx_wiki_generation_fragments_state
    ON wiki_generation_fragments (state);
