DO $$ BEGIN RAISE NOTICE '[Migration 000095] Dropping planned_name from tenant_skill_snapshots'; END $$;

ALTER TABLE tenant_skill_snapshots DROP COLUMN IF EXISTS planned_name;
