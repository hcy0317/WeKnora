DROP INDEX IF EXISTS idx_tenant_skills_catalog;
ALTER TABLE tenant_skills DROP COLUMN catalog_id;
DROP TABLE IF EXISTS tenant_skill_catalog;
