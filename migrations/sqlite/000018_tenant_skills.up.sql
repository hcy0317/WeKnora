-- Mirrors versioned migration 000093_tenant_skills for Lite deployments.

CREATE TABLE IF NOT EXISTS tenant_skills (
    id                    TEXT PRIMARY KEY,
    tenant_id             INTEGER NOT NULL,
    sandbox_config_id     TEXT NOT NULL,
    name                  TEXT NOT NULL,
    version               TEXT,
    description           TEXT,
    instructions          TEXT,
    bundle_ref            TEXT,
    bundle_sha256         TEXT,
    enabled               INTEGER NOT NULL DEFAULT 1,
    installed_snapshot_id TEXT,
    status                TEXT NOT NULL,
    error                 TEXT,
    installing_since      DATETIME,
    created_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at            DATETIME
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_tenant_skills_config_name
    ON tenant_skills (sandbox_config_id, name) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS tenant_skill_snapshots (
    id                 TEXT PRIMARY KEY,
    tenant_id          INTEGER NOT NULL,
    sandbox_config_id  TEXT NOT NULL,
    skill_id           TEXT,
    snapshot_id        TEXT,
    parent_snapshot_id TEXT,
    generation         INTEGER NOT NULL DEFAULT 0,
    trigger            TEXT NOT NULL,
    state              TEXT NOT NULL,
    superseded_at      DATETIME,
    created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tenant_skill_snapshots_config
    ON tenant_skill_snapshots (sandbox_config_id);
CREATE INDEX IF NOT EXISTS idx_tenant_skill_snapshots_state
    ON tenant_skill_snapshots (state);
