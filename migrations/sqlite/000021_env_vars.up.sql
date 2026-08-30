-- Mirrors versioned migration 000096_env_vars.

ALTER TABLE tenant_skills ADD COLUMN envs TEXT;

CREATE TABLE IF NOT EXISTS tenant_user_env_vars (
    id                TEXT PRIMARY KEY,
    tenant_id         INTEGER NOT NULL,
    principal_type    TEXT NOT NULL,
    principal_id      TEXT NOT NULL,
    sandbox_config_id TEXT NOT NULL,
    skill_id          TEXT NOT NULL DEFAULT '',
    name              TEXT NOT NULL,
    value             TEXT,
    created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_user_env_var
    ON tenant_user_env_vars (tenant_id, principal_type, principal_id, sandbox_config_id, skill_id, name);
CREATE INDEX IF NOT EXISTS idx_user_env_var_skill
    ON tenant_user_env_vars (tenant_id, skill_id);
CREATE INDEX IF NOT EXISTS idx_user_env_var_config
    ON tenant_user_env_vars (tenant_id, sandbox_config_id);
