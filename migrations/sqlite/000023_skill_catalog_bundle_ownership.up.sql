UPDATE tenant_skill_catalog
SET bundle_ref = NULL
WHERE bundle_ref IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM tenant_skills
      WHERE tenant_skills.tenant_id = tenant_skill_catalog.tenant_id
        AND tenant_skills.bundle_ref = tenant_skill_catalog.bundle_ref
        AND tenant_skills.deleted_at IS NULL
  );
