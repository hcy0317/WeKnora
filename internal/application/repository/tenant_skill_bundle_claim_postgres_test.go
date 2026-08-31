package repository

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestCatalogBundleClaimInterleavingPostgres(t *testing.T) {
	baseDSN := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if baseDSN == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("explicit ephemeral PostgreSQL test environment is required")
	}
	base, err := gorm.Open(postgres.Open(baseDSN), &gorm.Config{})
	require.NoError(t, err)
	schema := fmt.Sprintf("bundle_claim_%d", time.Now().UnixNano())
	require.NoError(t, base.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = base.Exec("DROP SCHEMA " + schema + " CASCADE").Error })

	parsed, err := url.Parse(baseDSN)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	dbA, err := gorm.Open(postgres.Open(parsed.String()), &gorm.Config{})
	require.NoError(t, err)
	dbB, err := gorm.Open(postgres.Open(parsed.String()), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, dbA.AutoMigrate(
		&types.TenantSkillEntity{}, &types.TenantSkillCatalogEntity{}, &skillBundleRefClaim{},
	))
	runCatalogBundleClaimInterleaving(t,
		NewTenantSkillRepository(dbA), NewTenantSkillRepository(dbB),
		func(ref string, age time.Duration) {
			require.NoError(t, dbA.Model(&skillBundleRefClaim{}).
				Where("tenant_id = ? AND bundle_ref = ?", 7, ref).
				Update("updated_at", time.Now().Add(-age)).Error)
		})
}

func TestSandboxCordonRenewCASPostgres(t *testing.T) {
	baseDSN := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if baseDSN == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("explicit ephemeral PostgreSQL test environment is required")
	}
	base, err := gorm.Open(postgres.Open(baseDSN), &gorm.Config{})
	require.NoError(t, err)
	schema := fmt.Sprintf("sandbox_cordon_%d", time.Now().UnixNano())
	require.NoError(t, base.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() { _ = base.Exec("DROP SCHEMA " + schema + " CASCADE").Error })
	parsed, err := url.Parse(baseDSN)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	dbA, err := gorm.Open(postgres.Open(parsed.String()), &gorm.Config{})
	require.NoError(t, err)
	dbB, err := gorm.Open(postgres.Open(parsed.String()), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, dbA.AutoMigrate(&types.TenantSandboxConfigEntity{}))
	repoA := NewTenantSandboxConfigRepository(dbA)
	repoB := NewTenantSandboxConfigRepository(dbB)
	ctx := context.Background()
	require.NoError(t, repoA.Create(ctx, &types.TenantSandboxConfigEntity{
		ID: "cfg-a", TenantID: 7, Name: "prod", SandboxType: "e2b",
		Config: &types.TenantSandboxConfig{SandboxType: "e2b"},
	}))
	oldToken := time.Now().Add(-types.SandboxCordonLease - time.Minute).UTC().Truncate(time.Microsecond)
	newToken := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, repoA.SetCordon(ctx, 7, "cfg-a", oldToken))
	require.NoError(t, repoB.RenewCordonIfMatch(ctx, 7, "cfg-a", oldToken, newToken))
	require.ErrorIs(t, repoA.RenewCordonIfMatch(
		ctx, 7, "cfg-a", oldToken, newToken.Add(time.Microsecond)), ErrSandboxConfigDeleteOwnerLost)
	row, err := repoA.GetByID(ctx, 7, "cfg-a")
	require.NoError(t, err)
	row.Description = "ordinary update must remain fenced"
	require.ErrorIs(t, repoA.Update(ctx, row), ErrSandboxConfigCordoned)
	require.ErrorIs(t, repoA.SoftDeleteCordoned(ctx, 7, "cfg-a", oldToken),
		ErrSandboxConfigDeleteOwnerLost)
}
