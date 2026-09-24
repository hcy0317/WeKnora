CREATE TABLE IF NOT EXISTS tenant_skill_bundle_ref_claims (
    tenant_id INTEGER NOT NULL,
    bundle_ref TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'live',
    claim_token TEXT,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, bundle_ref)
);

CREATE INDEX IF NOT EXISTS idx_skill_bundle_ref_claims_state_updated
    ON tenant_skill_bundle_ref_claims (state, updated_at);

WITH ranked AS (
    SELECT id, tenant_id, name, version, description, instructions,
           bundle_ref, bundle_sha256, created_at, updated_at,
           ROW_NUMBER() OVER (
               PARTITION BY tenant_id, name
               ORDER BY CASE WHEN bundle_ref IS NULL OR bundle_ref = '' THEN 1 ELSE 0 END,
                        updated_at DESC, created_at DESC
           ) AS rank
    FROM tenant_skills
    WHERE deleted_at IS NULL
)
INSERT OR IGNORE INTO tenant_skill_catalog (
    id, tenant_id, name, version, description, instructions,
    bundle_ref, bundle_sha256, created_at, updated_at
)
SELECT id, tenant_id, name, version, description, instructions,
       NULL, bundle_sha256, created_at, updated_at
FROM ranked
WHERE rank = 1;

UPDATE tenant_skills
SET catalog_id = (
    SELECT c.id
    FROM tenant_skill_catalog AS c
    WHERE c.deleted_at IS NULL
      AND c.tenant_id = tenant_skills.tenant_id
      AND c.name = tenant_skills.name
    LIMIT 1
)
WHERE deleted_at IS NULL
  AND (catalog_id IS NULL OR catalog_id = '');

UPDATE tenant_skill_catalog
SET bundle_ref = NULL
WHERE bundle_ref IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM tenant_skills
      WHERE tenant_skills.tenant_id = tenant_skill_catalog.tenant_id
        AND tenant_skills.bundle_ref = tenant_skill_catalog.bundle_ref
        AND tenant_skills.deleted_at IS NULL
  );