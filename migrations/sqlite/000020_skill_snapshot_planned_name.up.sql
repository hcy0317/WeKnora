-- Mirrors versioned migration 000095_skill_snapshot_planned_name.

ALTER TABLE tenant_skill_snapshots ADD COLUMN planned_name TEXT;
