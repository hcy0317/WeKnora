package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newResourceCatalogForTest(t *testing.T) (interfaces.ResourceCatalog, *gorm.DB) {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "resources.db") + "?_busy_timeout=5000&_journal_mode=WAL"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, db.AutoMigrate(&types.StoredResource{}, &types.ResourceBinding{}, &types.ResourceAccessGrant{}))
	return NewResourceCatalog(repository.NewResourceRepository(db)), db
}

func TestResourceCatalogRegisterResolveAndDeduplicate(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	physical := "storage://backend-a/local://7/exports/a.png"

	ref, err := catalog.Register(ctx, 7, physical, interfaces.ResourceRegistration{Kind: "image", OriginalName: "a.png"})
	require.NoError(t, err)
	require.Regexp(t, `^resource://[0-9A-Za-z_-]{22}$`, ref)

	again, err := catalog.Register(ctx, 7, physical, interfaces.ResourceRegistration{})
	require.NoError(t, err)
	require.Equal(t, ref, again)

	resolvedPath, resource, err := catalog.ResolvePath(ctx, ref)
	require.NoError(t, err)
	require.Equal(t, physical, resolvedPath)
	require.Equal(t, uint64(7), resource.TenantID)
	require.Equal(t, "backend-a", resource.StorageBackendID)
	require.Equal(t, "local", resource.Provider)
}

func TestResourceCatalogBindingAndAccessGrant(t *testing.T) {
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(
		ctx,
		9,
		"local://9/exports/report.pdf",
		interfaces.ResourceRegistration{OriginalName: "report.pdf"},
	)
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(ctx, ref, "knowledge", "knowledge-1", "source_file"))

	token, err := catalog.CreateAccessGrant(ctx, ref, time.Minute)
	require.NoError(t, err)
	require.Len(t, token, 22)
	var storedGrant types.ResourceAccessGrant
	require.NoError(t, db.First(&storedGrant).Error)
	require.NotEqual(t, token, storedGrant.TokenHash)
	require.Len(t, storedGrant.TokenHash, 64)
	resource, err := catalog.ResolveAccessGrant(ctx, token)
	require.NoError(t, err)
	require.Equal(t, uint64(9), resource.TenantID)
}

// The two-owner case that "save this answer to the knowledge base" creates:
// one blob, claimed by both the assistant message and the new document.
// Deleting either owner must leave the other's copy intact.
func TestResourceCatalogReleaseKeepsFileWhileAnotherOwnerClaimsIt(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/chart.html", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerMessage, "msg-1", types.ResourceRelationArtifact))
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "kn-1", types.ResourceRelationAttachment))

	remaining, _, err := catalog.Release(ctx, ref, types.ResourceOwnerKnowledge, "kn-1")
	require.NoError(t, err)
	require.EqualValues(t, 1, remaining, "the message still shows this file")

	remaining, _, err = catalog.Release(ctx, ref, types.ResourceOwnerMessage, "msg-1")
	require.NoError(t, err)
	require.EqualValues(t, 0, remaining, "the last claim is gone, the bytes can go too")
}

// Binding twice is how a republished document re-takes its claim, and it must
// not inflate the count into a file that can never be collected.
func TestResourceCatalogBindIsIdempotentPerOwner(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/a.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "kn-1", types.ResourceRelationAttachment))
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "kn-1", types.ResourceRelationAttachment))

	remaining, _, err := catalog.Release(ctx, ref, types.ResourceOwnerKnowledge, "kn-1")
	require.NoError(t, err)
	require.EqualValues(t, 0, remaining)
}

func TestResourceCatalogDeletionClaimSerializesWithBinding(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/race.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerKnowledge, "kn-1", types.ResourceRelationAttachment))
	require.NoError(t, catalog.Bind(ctx, ref, types.ResourceOwnerMessage, "msg-1", types.ResourceRelationArtifact))

	remaining, _, err := catalog.Release(ctx, ref, types.ResourceOwnerKnowledge, "kn-1")
	require.NoError(t, err)
	require.EqualValues(t, 1, remaining)

	// The existing binding winner keeps the resource active.
	_, err = catalog.MarkDeleted(ctx, ref)
	require.Error(t, err)
	_, _, err = catalog.Release(ctx, ref, types.ResourceOwnerMessage, "msg-1")
	require.NoError(t, err)

	// Releasing the last binding claims the tombstone in the same transaction;
	// later binders cannot resurrect the resource.
	require.Error(t, catalog.Bind(ctx, ref, types.ResourceOwnerMessage, "msg-2", types.ResourceRelationArtifact))
	_, err = catalog.Resolve(ctx, ref)
	require.Error(t, err)
}

func TestResourceCatalogConcurrentBindAndDeleteNeverLeavesDeletedBinding(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		catalog, db := newResourceCatalogForTest(t)
		ctx := context.Background()
		ref, err := catalog.Register(ctx, 7,
			fmt.Sprintf("local://7/exports/race-%d.png", iteration), interfaces.ResourceRegistration{})
		require.NoError(t, err)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		var bindErr, deleteErr error
		go func() {
			defer wg.Done()
			<-start
			bindErr = catalog.Bind(ctx, ref, types.ResourceOwnerMessage, "msg-1", types.ResourceRelationArtifact)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, deleteErr = catalog.MarkDeleted(ctx, ref)
		}()
		close(start)
		wg.Wait()

		handle, ok := types.ParseResourcePath(ref)
		require.True(t, ok)
		var resource types.StoredResource
		require.NoError(t, db.Unscoped().Where("handle = ?", handle).First(&resource).Error)
		var bindings int64
		require.NoError(t, db.Model(&types.ResourceBinding{}).
			Where("resource_id = ?", resource.ID).Count(&bindings).Error)
		if resource.State == types.ResourceStateDeleted {
			require.Zero(t, bindings, "a deletion winner must exclude concurrent binding")
			require.Error(t, bindErr)
			require.NoError(t, deleteErr)
		} else {
			require.EqualValues(t, 1, bindings, "a binding winner must keep the resource active")
			require.NoError(t, bindErr)
			require.Error(t, deleteErr)
		}
	}
}

func TestResourceCatalogDeletionClaimMustMatchRestore(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 7, "local://7/exports/claim.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	first, err := catalog.MarkDeleted(ctx, ref)
	require.NoError(t, err)
	require.False(t, first.IsZero())
	require.Error(t, catalog.RestoreActive(ctx, ref, first.Add(time.Microsecond)))
	require.NoError(t, catalog.ValidateDeletionClaim(ctx, ref, first))
	require.NoError(t, catalog.RestoreActive(ctx, ref, first))

	second, err := catalog.MarkDeleted(ctx, ref)
	require.NoError(t, err)
	require.NotEqual(t, first, second, "each deletion claim must uniquely fence older callers")
	require.Error(t, catalog.ValidateDeletionClaim(ctx, ref, first))
	require.NoError(t, catalog.ValidateDeletionClaim(ctx, ref, second))
}

// Releasing something that was never bound, or is not a handle at all, must be
// harmless: callers release optimistically from a content scan.
func TestResourceCatalogReleaseUnknownReference(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()

	remaining, _, err := catalog.Release(ctx, "local://7/exports/legacy.png", types.ResourceOwnerKnowledge, "kn-1")
	require.NoError(t, err)
	require.EqualValues(t, -1, remaining, "a raw provider path has no claims to account for")

	ref, err := catalog.Register(ctx, 7, "local://7/exports/b.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	remaining, _, err = catalog.Release(ctx, ref, types.ResourceOwnerKnowledge, "never-bound")
	require.NoError(t, err)
	require.EqualValues(t, 0, remaining)

	_, _, err = catalog.Release(ctx, ref, "", "kn-1")
	require.Error(t, err, "an owner is required to release a claim")
}

// Rendering one answer resolves the same image many times, and re-reading a
// message history resolves it again on every call. Each resolution used to
// insert a capability row; a live one must be reused instead.
func TestResourceCatalogReusesLiveAccessGrant(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "weknora-test-aes-key-32bytes!!!")
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 9, "local://9/exports/a.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	first, err := catalog.CreateAccessGrant(ctx, ref, time.Hour)
	require.NoError(t, err)
	second, err := catalog.CreateAccessGrant(ctx, ref, time.Hour)
	require.NoError(t, err)

	require.Equal(t, first, second)
	var grants int64
	require.NoError(t, db.Model(&types.ResourceAccessGrant{}).Count(&grants).Error)
	require.EqualValues(t, 1, grants, "a live grant must be reused, not duplicated")

	resource, err := catalog.ResolveAccessGrant(ctx, second)
	require.NoError(t, err)
	require.Equal(t, uint64(9), resource.TenantID)
}

// Two resources must never share a grant.
func TestResourceCatalogGrantsArePerResource(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "weknora-test-aes-key-32bytes!!!")
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	first, err := catalog.Register(ctx, 9, "local://9/exports/a.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)
	second, err := catalog.Register(ctx, 9, "local://9/exports/b.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	firstToken, err := catalog.CreateAccessGrant(ctx, first, time.Hour)
	require.NoError(t, err)
	secondToken, err := catalog.CreateAccessGrant(ctx, second, time.Hour)
	require.NoError(t, err)
	require.NotEqual(t, firstToken, secondToken)
}

// Revoking a grant must stick: the derived token would otherwise recompute to
// the same value and a fresh insert would revive the access it just lost.
func TestResourceCatalogDoesNotReviveRevokedGrant(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "weknora-test-aes-key-32bytes!!!")
	catalog, db := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 9, "local://9/exports/a.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	revoked, err := catalog.CreateAccessGrant(ctx, ref, time.Hour)
	require.NoError(t, err)
	now := time.Now().UTC()
	require.NoError(t, db.Model(&types.ResourceAccessGrant{}).
		Where("1 = 1").Update("revoked_at", &now).Error)

	fresh, err := catalog.CreateAccessGrant(ctx, ref, time.Hour)
	require.NoError(t, err)
	require.NotEqual(t, revoked, fresh, "a revoked token must not be handed out again")

	_, err = catalog.ResolveAccessGrant(ctx, revoked)
	require.Error(t, err, "the revoked token must stay unusable")
	resource, err := catalog.ResolveAccessGrant(ctx, fresh)
	require.NoError(t, err)
	require.Equal(t, uint64(9), resource.TenantID)
}

// Without a signing key the deployment cannot derive tokens, so grants stay
// random and per-request — the behaviour before reuse existed.
func TestResourceCatalogWithoutSigningKeyMintsFreshGrants(t *testing.T) {
	t.Setenv("SYSTEM_AES_KEY", "")
	catalog, _ := newResourceCatalogForTest(t)
	ctx := context.Background()
	ref, err := catalog.Register(ctx, 9, "local://9/exports/a.png", interfaces.ResourceRegistration{})
	require.NoError(t, err)

	first, err := catalog.CreateAccessGrant(ctx, ref, time.Hour)
	require.NoError(t, err)
	second, err := catalog.CreateAccessGrant(ctx, ref, time.Hour)
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestResourceCatalogRejectsUnsupportedPhysicalPath(t *testing.T) {
	catalog, _ := newResourceCatalogForTest(t)
	_, err := catalog.Register(context.Background(), 7, "https://example.com/a.png", interfaces.ResourceRegistration{})
	require.ErrorContains(t, err, "unsupported provider")
}
