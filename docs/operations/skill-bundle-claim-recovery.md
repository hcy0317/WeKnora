# Skill bundle claim recovery

Skill and catalog archives share the durable
`tenant_skill_bundle_ref_claims` fence. A `writing` claim older than two
minutes is recoverable by the next writer: publication is token-CAS fenced and
the winner stores the archive again before publishing the reference.

A stale `deleting` claim is deliberately not lease-recoverable. Object stores
do not expose a delete generation through `FileService.DeleteFile`; releasing
that claim while an old delete call might still be running could let it remove
bytes stored by a new writer.

## Recover a stale deleting claim

1. Stop every WeKnora application instance that can install, register, remove,
   or reap skills. Verify they are stopped before continuing.
2. Read the exact claim and retain its `claim_token`:

   ```sql
   SELECT tenant_id, bundle_ref, state, claim_token, updated_at
   FROM tenant_skill_bundle_ref_claims
   WHERE state = 'deleting'
   ORDER BY updated_at;
   ```

3. For the selected `(tenant_id, bundle_ref)`, verify both queries return zero:

   ```sql
   SELECT COUNT(*) FROM tenant_skills
   WHERE tenant_id = :tenant_id AND bundle_ref = :bundle_ref AND deleted_at IS NULL;

   SELECT COUNT(*) FROM tenant_skill_catalog
   WHERE tenant_id = :tenant_id AND bundle_ref = :bundle_ref AND deleted_at IS NULL;
   ```

4. Delete the exact object named by `bundle_ref` through the configured tenant
   storage backend. Do not release the claim if deletion cannot be verified.
5. Complete, rather than clear, the same token-CAS claim:

   ```sql
   UPDATE tenant_skill_bundle_ref_claims
   SET state = 'deleted', claim_token = NULL, updated_at = CURRENT_TIMESTAMP
   WHERE tenant_id = :tenant_id
     AND bundle_ref = :bundle_ref
     AND state = 'deleting'
     AND claim_token = :observed_claim_token;
   ```

   Exactly one row must be updated. A zero-row result means ownership changed;
   stop and re-audit from step 2.
6. Restart the application instances. A future writer that reuses the ref will
   acquire `writing`, store the bytes again, and token-CAS publish the DB ref.
