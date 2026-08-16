-- Durable Wiki identity registry and automatic-merge audit trail.
CREATE TABLE IF NOT EXISTS wiki_canonical_identities (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    page_type VARCHAR(32) NOT NULL,
    identity_key VARCHAR(512) NOT NULL,
    canonical_slug VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_wiki_canonical_identity UNIQUE (knowledge_base_id, page_type, identity_key)
);
CREATE INDEX IF NOT EXISTS idx_wiki_canonical_tenant_kb
    ON wiki_canonical_identities (tenant_id, knowledge_base_id);

CREATE INDEX IF NOT EXISTS idx_wiki_pages_canonical_identity_lookup
    ON wiki_pages (
        knowledge_base_id,
        page_type,
        (regexp_replace(lower(trim(title)), '[^[:alnum:]]+', '', 'g'))
    )
    WHERE deleted_at IS NULL
      AND status <> 'archived'
      AND page_type IN ('entity', 'concept');

CREATE TABLE IF NOT EXISTS wiki_page_merge_audits (
    id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    knowledge_base_id VARCHAR(36) NOT NULL,
    page_type VARCHAR(32) NOT NULL,
    identity_key VARCHAR(512) NOT NULL,
    canonical_page_id VARCHAR(36) NOT NULL,
    canonical_slug VARCHAR(255) NOT NULL,
    merged_page_id VARCHAR(36) NOT NULL UNIQUE,
    merged_slug VARCHAR(255) NOT NULL,
    reason VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_wiki_merge_audits_kb
    ON wiki_page_merge_audits (knowledge_base_id, created_at);

-- Seed every pre-existing identity deterministically. More source coverage,
-- then higher revision and richer content win; lexical slug is the final tie.
INSERT INTO wiki_canonical_identities (
    tenant_id, knowledge_base_id, page_type, identity_key, canonical_slug, created_at, updated_at
)
SELECT tenant_id, knowledge_base_id, page_type, identity_key, slug, NOW(), NOW()
FROM (
    SELECT tenant_id, knowledge_base_id, page_type, slug,
           regexp_replace(lower(trim(title)), '[^[:alnum:]]+', '', 'g') AS identity_key,
           ROW_NUMBER() OVER (
               PARTITION BY knowledge_base_id, page_type,
                   regexp_replace(lower(trim(title)), '[^[:alnum:]]+', '', 'g')
               ORDER BY jsonb_array_length(COALESCE(source_refs, '[]'::jsonb)) DESC,
                        version DESC, length(content) DESC, created_at ASC, slug ASC
           ) AS rank
    FROM wiki_pages
    WHERE deleted_at IS NULL
      AND status <> 'archived'
      AND page_type IN ('entity', 'concept')
) ranked
WHERE rank = 1 AND identity_key <> ''
ON CONFLICT (knowledge_base_id, page_type, identity_key) DO NOTHING;

-- Let the normal durable finalize/recovery mechanism perform the first
-- high-confidence historical convergence after the application starts.
INSERT INTO task_pending_ops (
    tenant_id, task_type, scope, scope_id, op, dedup_key, payload, fail_count, enqueued_at
)
SELECT kb.tenant_id, 'wiki:finalize', 'knowledge_base', kb.id,
       'canonical_reconcile', 'canonical_reconcile', '{}'::jsonb, 0, NOW()
FROM knowledge_bases kb
WHERE kb.deleted_at IS NULL
  AND COALESCE((kb.indexing_strategy->>'wiki_enabled')::boolean, false)
  AND NOT EXISTS (
      SELECT 1 FROM task_pending_ops pending
      WHERE pending.task_type = 'wiki:finalize'
        AND pending.scope = 'knowledge_base'
        AND pending.scope_id = kb.id
        AND pending.op = 'canonical_reconcile'
  );
