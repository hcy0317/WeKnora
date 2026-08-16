CREATE TABLE IF NOT EXISTS wiki_canonical_identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    page_type TEXT NOT NULL,
    identity_key TEXT NOT NULL,
    canonical_slug TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_wiki_canonical_identity UNIQUE (knowledge_base_id, page_type, identity_key)
);
CREATE INDEX IF NOT EXISTS idx_wiki_canonical_tenant_kb
    ON wiki_canonical_identities (tenant_id, knowledge_base_id);

CREATE TABLE IF NOT EXISTS wiki_page_merge_audits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id INTEGER NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    page_type TEXT NOT NULL,
    identity_key TEXT NOT NULL,
    canonical_page_id TEXT NOT NULL,
    canonical_slug TEXT NOT NULL,
    merged_page_id TEXT NOT NULL UNIQUE,
    merged_slug TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_wiki_merge_audits_kb
    ON wiki_page_merge_audits (knowledge_base_id, created_at);

-- SQLite has no built-in Unicode category matcher equivalent to Go's
-- unicode.IsLetter/IsDigit. Populate this registry lazily through the
-- application so every deployment uses exactly the same identity key.
