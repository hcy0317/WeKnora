CREATE TABLE IF NOT EXISTS tenant_skill_bundle_ref_claims (
    tenant_id BIGINT NOT NULL,
    bundle_ref TEXT NOT NULL,
    state VARCHAR(16) NOT NULL DEFAULT 'live',
    claim_token VARCHAR(64),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, bundle_ref)
);

CREATE INDEX IF NOT EXISTS idx_skill_bundle_ref_claims_state_updated
    ON tenant_skill_bundle_ref_claims (state, updated_at);
