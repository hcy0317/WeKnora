CREATE TABLE IF NOT EXISTS question_generation_manifests (
    id VARCHAR(36) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_id VARCHAR(36) NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    chunk_id VARCHAR(36) NOT NULL,
    content_revision INT NOT NULL,
    batch_index INT NOT NULL,
    attempt INT NOT NULL DEFAULT 0,
    identity_version INT NOT NULL,
    generation_key VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    vector_store_id VARCHAR(36) NOT NULL DEFAULT '',
    embedding_model_id VARCHAR(36) NOT NULL,
    embedding_dimension INT NOT NULL,
    knowledge_type VARCHAR(32) NOT NULL,
    effective_engines JSONB NOT NULL,
    state VARCHAR(32) NOT NULL,
    questions JSONB NOT NULL,
    index_entries JSONB NOT NULL,
    desired_source_ids JSONB NOT NULL,
    abandoned_source_ids JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_question_generation_manifest
        UNIQUE (tenant_id, knowledge_id, chunk_id, content_revision, batch_index)
);

CREATE INDEX IF NOT EXISTS idx_question_generation_manifests_knowledge
    ON question_generation_manifests (tenant_id, knowledge_id);
CREATE INDEX IF NOT EXISTS idx_question_generation_manifests_chunk
    ON question_generation_manifests (tenant_id, chunk_id);
CREATE INDEX IF NOT EXISTS idx_question_generation_manifests_generation_key
    ON question_generation_manifests (generation_key);
