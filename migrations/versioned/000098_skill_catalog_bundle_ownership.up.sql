-- Catalog bundles are independently owned. Older v97 deployments copied the
-- installation ref into the catalog without reference accounting; clear only
-- proven shared refs so lazy read/removal backfill can create a catalog copy.
UPDATE tenant_skill_catalog AS catalog
SET bundle_ref = NULL
WHERE bundle_ref IS NOT NULL
  AND EXISTS (
      SELECT 1
      FROM tenant_skills AS skill
      WHERE skill.tenant_id = catalog.tenant_id
        AND skill.bundle_ref = catalog.bundle_ref
        AND skill.deleted_at IS NULL
  );
