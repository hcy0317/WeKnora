CREATE TABLE IF NOT EXISTS question_generation_manifests (
    id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    chunk_id TEXT NOT NULL,
    content_revision INTEGER NOT NULL,
    batch_index INTEGER NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    identity_version INTEGER NOT NULL,
    generation_key TEXT NOT NULL,
    task_id TEXT NOT NULL,
    vector_store_id TEXT NOT NULL DEFAULT '',
    embedding_model_id TEXT NOT NULL,
    embedding_dimension INTEGER NOT NULL,
    knowledge_type TEXT NOT NULL,
    effective_engines JSON NOT NULL,
    state TEXT NOT NULL,
    questions JSON NOT NULL,
    index_entries JSON NOT NULL,
    desired_source_ids JSON NOT NULL,
    abandoned_source_ids JSON NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_question_generation_manifest
        UNIQUE (tenant_id, knowledge_id, chunk_id, content_revision, batch_index)
);

CREATE INDEX IF NOT EXISTS idx_question_generation_manifests_knowledge
    ON question_generation_manifests (tenant_id, knowledge_id);
CREATE INDEX IF NOT EXISTS idx_question_generation_manifests_chunk
    ON question_generation_manifests (tenant_id, chunk_id);
CREATE INDEX IF NOT EXISTS idx_question_generation_manifests_generation_key
    ON question_generation_manifests (generation_key);
