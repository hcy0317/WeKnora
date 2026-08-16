DELETE FROM task_pending_ops
WHERE task_type = 'wiki:finalize' AND op = 'canonical_reconcile';

DROP INDEX IF EXISTS idx_wiki_pages_canonical_identity_lookup;
DROP TABLE IF EXISTS wiki_page_merge_audits;
DROP TABLE IF EXISTS wiki_canonical_identities;
