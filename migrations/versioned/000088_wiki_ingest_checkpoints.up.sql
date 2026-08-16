CREATE TABLE IF NOT EXISTS wiki_ingest_work_units (
    work_id VARCHAR(64) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    knowledge_id VARCHAR(36) NOT NULL,
    source_revision_digest VARCHAR(64) NOT NULL,
    source_document_key VARCHAR(64) NOT NULL,
    generation_contract_key VARCHAR(64) NOT NULL,
    runtime_snapshot_key VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    mapped_output JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wiki_work_source
    ON wiki_ingest_work_units (tenant_id, knowledge_base_id, knowledge_id);

CREATE TABLE IF NOT EXISTS wiki_taxonomy_plans (
    plan_id VARCHAR(64) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    work_set_digest VARCHAR(64) NOT NULL,
    missing_set_digest VARCHAR(64) NOT NULL,
    folder_base_digest VARCHAR(64) NOT NULL,
    contract_key VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    resolved_output JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wiki_taxonomy_plans_kb
    ON wiki_taxonomy_plans (tenant_id, knowledge_base_id);
CREATE INDEX IF NOT EXISTS idx_wiki_taxonomy_plans_stable
    ON wiki_taxonomy_plans (tenant_id, knowledge_base_id, work_set_digest, missing_set_digest, contract_key, state);

CREATE TABLE IF NOT EXISTS wiki_slug_applications (
    plan_id VARCHAR(64) PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    contribution_key VARCHAR(64) NOT NULL,
    expected_version INT NOT NULL,
    expected_page_hash VARCHAR(64) NOT NULL,
    operation_digest VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    generated_output TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_wiki_slug_applications_lookup
    ON wiki_slug_applications (tenant_id, knowledge_base_id, slug, contribution_key);

CREATE TABLE IF NOT EXISTS wiki_slug_contribution_markers (
    id BIGSERIAL PRIMARY KEY,
    plan_id VARCHAR(64) NOT NULL,
    work_id VARCHAR(64) NOT NULL,
    slug VARCHAR(255) NOT NULL,
    operation_digest VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_wiki_slug_contribution UNIQUE (work_id, slug, operation_digest)
);

CREATE INDEX IF NOT EXISTS idx_wiki_slug_contribution_plan
    ON wiki_slug_contribution_markers (plan_id);
