CREATE TABLE IF NOT EXISTS wiki_ingest_work_units (
    work_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    knowledge_id TEXT NOT NULL,
    source_revision_digest TEXT NOT NULL,
    source_document_key TEXT NOT NULL,
    generation_contract_key TEXT NOT NULL,
    runtime_snapshot_key TEXT NOT NULL,
    state TEXT NOT NULL,
    mapped_output JSON NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_wiki_work_source
    ON wiki_ingest_work_units (tenant_id, knowledge_base_id, knowledge_id);

CREATE TABLE IF NOT EXISTS wiki_taxonomy_plans (
    plan_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    work_set_digest TEXT NOT NULL,
    missing_set_digest TEXT NOT NULL,
    folder_base_digest TEXT NOT NULL,
    contract_key TEXT NOT NULL,
    state TEXT NOT NULL,
    resolved_output JSON NOT NULL DEFAULT '{}',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_wiki_taxonomy_plans_kb
    ON wiki_taxonomy_plans (tenant_id, knowledge_base_id);
CREATE INDEX IF NOT EXISTS idx_wiki_taxonomy_plans_stable
    ON wiki_taxonomy_plans (tenant_id, knowledge_base_id, work_set_digest, missing_set_digest, contract_key, state);

CREATE TABLE IF NOT EXISTS wiki_slug_applications (
    plan_id TEXT PRIMARY KEY,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    contribution_key TEXT NOT NULL,
    expected_version INTEGER NOT NULL,
    expected_page_hash TEXT NOT NULL,
    operation_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    generated_output TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_wiki_slug_applications_lookup
    ON wiki_slug_applications (tenant_id, knowledge_base_id, slug, contribution_key);

CREATE TABLE IF NOT EXISTS wiki_slug_contribution_markers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id TEXT NOT NULL,
    work_id TEXT NOT NULL,
    slug TEXT NOT NULL,
    operation_digest TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_wiki_slug_contribution UNIQUE (work_id, slug, operation_digest)
);
CREATE INDEX IF NOT EXISTS idx_wiki_slug_contribution_plan
    ON wiki_slug_contribution_markers (plan_id);
