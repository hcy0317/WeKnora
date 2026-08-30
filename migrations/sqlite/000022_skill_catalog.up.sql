-- Mirrors versioned migration 000097_skill_catalog.

CREATE TABLE IF NOT EXISTS tenant_skill_catalog (
    id            TEXT PRIMARY KEY,
    tenant_id     INTEGER NOT NULL,
    name          TEXT NOT NULL,
    version       TEXT,
    description   TEXT,
    instructions  TEXT,
    bundle_ref    TEXT,
    bundle_sha256 TEXT,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at    DATETIME
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_skill_catalog_name
    ON tenant_skill_catalog (tenant_id, name) WHERE deleted_at IS NULL;

ALTER TABLE tenant_skills ADD COLUMN catalog_id TEXT;
CREATE INDEX IF NOT EXISTS idx_tenant_skills_catalog ON tenant_skills (catalog_id);

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
