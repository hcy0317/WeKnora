CREATE TABLE IF NOT EXISTS tenant_skill_bundle_ref_claims (
    tenant_id INTEGER NOT NULL,
    bundle_ref TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'live',
    claim_token TEXT,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, bundle_ref)
);
