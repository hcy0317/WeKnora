package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/Tencent/WeKnora/internal/application/repository"
	apperrors "github.com/Tencent/WeKnora/internal/errors"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestListCatalogGroupsInstallsByDefinition(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:catalog-list?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&types.TenantSkillEntity{}, &types.TenantSkillCatalogEntity{},
		&types.TenantSkillSnapshotEntity{}, &types.TenantUserEnvVar{},
	))
	repo := repository.NewTenantSkillRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleAdmin)

	require.NoError(t, repo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-pdf", TenantID: 7, Name: "pdf", Description: "extract",
	}))
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-1", TenantID: 7, SandboxConfigID: "cfg-a", CatalogID: "cat-pdf",
		Name: "pdf", Status: types.SkillStatusReady, Enabled: true,
	}))
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-2", TenantID: 7, SandboxConfigID: "cfg-b", CatalogID: "cat-pdf",
		Name: "pdf", Status: types.SkillStatusInstalling, Enabled: true,
	}))

	svc := NewTenantSkillService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	list, err := svc.ListCatalog(ctx, 7)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "pdf", list[0].Name)
	require.Len(t, list[0].Installations, 2)
}

func TestResolveCatalogFindsLegacySkillID(t *testing.T) {
	db, err := gorm.Open(sqlite.Open("file:catalog-legacy?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&types.TenantSkillEntity{}, &types.TenantSkillCatalogEntity{},
		&types.TenantSkillSnapshotEntity{}, &types.TenantUserEnvVar{},
	))
	repo := repository.NewTenantSkillRepository(db)
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleAdmin)
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-old", TenantID: 7, SandboxConfigID: "cfg-a",
		Name: "pdf", BundleRef: "local://7/tenant-skills/sk-old.zip",
		Status: types.SkillStatusReady, Enabled: true,
	}))

	svc := NewTenantSkillService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	cat, err := svc.resolveCatalog(ctx, 7, "sk-old")
	require.NoError(t, err)
	require.NotNil(t, cat)
	require.Equal(t, "pdf", cat.Name)
	require.Empty(t, cat.BundleRef,
		"legacy projection must not make the catalog co-own the install bundle")
}

func catalogTestRepo(t *testing.T, dsn string) repository.TenantSkillRepository {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(
		&types.TenantSkillEntity{}, &types.TenantSkillCatalogEntity{},
		&types.TenantSkillSnapshotEntity{}, &types.TenantUserEnvVar{},
	))
	return repository.NewTenantSkillRepository(db)
}

func TestListCatalogShowsInstallsWhoseCatalogWasDeleted(t *testing.T) {
	repo := catalogTestRepo(t, "file:catalog-orphan?mode=memory&cache=shared")
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleAdmin)
	require.NoError(t, repo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-gone", TenantID: 7, Name: "pdf",
	}))
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-1", TenantID: 7, SandboxConfigID: "cfg-a", CatalogID: "cat-gone",
		Name: "pdf", Status: types.SkillStatusReady, Enabled: true,
	}))
	require.NoError(t, repo.DeleteCatalog(ctx, 7, "cat-gone"))

	svc := NewTenantSkillService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	list, err := svc.ListCatalog(ctx, 7)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Equal(t, "pdf", list[0].Name)
	require.Len(t, list[0].Installations, 1)
	require.Equal(t, "sk-1", list[0].Installations[0].SkillID)
}

func TestListCatalogViewerDoesNotExposeOperationalState(t *testing.T) {
	repo := catalogTestRepo(t, "file:catalog-viewer?mode=memory&cache=shared")
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleViewer)
	require.NoError(t, repo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-pdf", TenantID: 7, Name: "pdf", BundleSHA256: strings.Repeat("a", 64),
	}))
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-1", TenantID: 7, SandboxConfigID: "cfg-a", CatalogID: "cat-pdf",
		Name: "pdf", Status: types.SkillStatusFailed, Error: "private provider detail",
		BundleSHA256: strings.Repeat("b", 64), Enabled: true,
	}))

	svc := NewTenantSkillService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	list, err := svc.ListCatalog(ctx, 7)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Empty(t, list[0].BundleSHA256)
	require.Nil(t, list[0].Installations)
}

func TestListCatalogContributorCanSelectInstalledSkillWithoutOperationalSecrets(t *testing.T) {
	repo := catalogTestRepo(t, "file:catalog-contributor?mode=memory&cache=shared")
	ctx := context.WithValue(context.Background(), types.TenantRoleContextKey, types.TenantRoleContributor)
	require.NoError(t, repo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-pdf", TenantID: 7, Name: "pdf", BundleSHA256: strings.Repeat("a", 64),
	}))
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-1", TenantID: 7, SandboxConfigID: "cfg-a", CatalogID: "cat-pdf",
		Name: "pdf", Status: types.SkillStatusReady, Error: "private provider detail",
		BundleSHA256: strings.Repeat("b", 64), Enabled: true,
	}))

	svc := NewTenantSkillService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	list, err := svc.ListCatalog(ctx, 7)
	require.NoError(t, err)
	require.Len(t, list, 1)
	require.Empty(t, list[0].BundleSHA256)
	require.Len(t, list[0].Installations, 1)
	install := list[0].Installations[0]
	require.Equal(t, "cfg-a", install.SandboxConfigID)
	require.Equal(t, types.SkillStatusReady, install.Status)
	require.True(t, install.Enabled)
	require.Empty(t, install.SkillID)
	require.Empty(t, install.Error)
	require.Empty(t, install.BundleSHA256)
	encoded, err := json.Marshal(list[0])
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "bundle_sha256")
	require.NotContains(t, string(encoded), "skill_id")
	require.NotContains(t, string(encoded), "private provider detail")
}

func TestDeleteCatalogRefusesWhileARemovalIsInFlight(t *testing.T) {
	repo := catalogTestRepo(t, "file:catalog-removing?mode=memory&cache=shared")
	ctx := context.Background()
	require.NoError(t, repo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-pdf", TenantID: 7, Name: "pdf",
	}))
	require.NoError(t, repo.CreateSkill(ctx, &types.TenantSkillEntity{
		ID: "sk-1", TenantID: 7, SandboxConfigID: "cfg-a", CatalogID: "cat-pdf",
		Name: "pdf", Status: types.SkillStatusRemoving, Enabled: true,
	}))

	svc := NewTenantSkillService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	err := svc.DeleteCatalog(ctx, 7, "cat-pdf")
	require.Error(t, err)
	appErr, ok := apperrors.IsAppError(err)
	require.True(t, ok)
	require.Equal(t, 409, appErr.HTTPCode)
}

func TestUpsertCatalogDoesNotStampSHAWhenStoreFails(t *testing.T) {
	fx := newInstallFixture(t)
	fx.saveErr = errors.New("disk full")
	ctx := context.Background()
	oldSHA := strings.Repeat("c", 64)
	require.NoError(t, fx.skillRepo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-1", TenantID: 7, Name: "pdf-tools",
		BundleRef: "file://old.zip", BundleSHA256: oldSHA,
	}))
	archive := zipBundle(t, map[string]string{"SKILL.md": validSkillMD})
	bundle, err := ParseSkillBundle(archive)
	require.NoError(t, err)
	require.NotEqual(t, oldSHA, bundle.SHA256)

	got, err := fx.svc.upsertCatalogFromBundle(ctx, 7, bundle, archive, false)
	require.NoError(t, err)
	require.Equal(t, oldSHA, got.BundleSHA256)
	require.Equal(t, "file://old.zip", got.BundleRef)
}

func TestCatalogReplacementDeletesOnlyCatalogOwnedBundle(t *testing.T) {
	fx := newInstallFixture(t)
	fx.returnNamedBundleRefs = true
	ctx := context.Background()
	installRef := "file://tenant-skills/sk-1/install.zip"
	skill, err := fx.skillRepo.GetSkill(ctx, 7, "cfg-1", "sk-1")
	require.NoError(t, err)
	skill.BundleRef = installRef
	require.NoError(t, fx.skillRepo.UpdateSkill(ctx, skill))

	firstArchive := zipBundle(t, map[string]string{"SKILL.md": validSkillMD})
	first, err := ParseSkillBundle(firstArchive)
	require.NoError(t, err)
	cat, err := fx.svc.upsertCatalogFromBundle(ctx, 7, first, firstArchive, true)
	require.NoError(t, err)
	firstCatalogRef := cat.BundleRef
	require.NotEqual(t, installRef, firstCatalogRef)

	secondArchive := zipBundle(t, map[string]string{
		"SKILL.md": validSkillMD, "scripts/new.py": "print('new')\n",
	})
	second, err := ParseSkillBundle(secondArchive)
	require.NoError(t, err)
	cat, err = fx.svc.upsertCatalogFromBundle(ctx, 7, second, secondArchive, true)
	require.NoError(t, err)
	require.NotEqual(t, firstCatalogRef, cat.BundleRef)
	require.Contains(t, fx.deletedBundles, firstCatalogRef)
	require.NotContains(t, fx.deletedBundles, installRef)

	require.NoError(t, fx.svc.DeleteCatalog(ctx, 7, cat.ID))
	require.Contains(t, fx.deletedBundles, cat.BundleRef)
	require.NotContains(t, fx.deletedBundles, installRef)
}

func TestMigratedCatalogBackfillsBeforeLastRemovalAndCanReinstall(t *testing.T) {
	fx := newInstallFixture(t)
	fx.returnNamedBundleRefs = true
	ctx := context.Background()
	archive := zipBundle(t, map[string]string{"SKILL.md": validSkillMD})
	sha := skillArchiveSHA256(archive)
	installRef := "file://tenant-skills/sk-1/" + sha + ".zip"
	fx.storedBundles = map[string][]byte{installRef: archive}
	require.NoError(t, fx.skillRepo.CreateCatalog(ctx, &types.TenantSkillCatalogEntity{
		ID: "cat-migrated", TenantID: 7, Name: "pdf-tools", BundleSHA256: sha,
	}))
	skill, err := fx.skillRepo.GetSkill(ctx, 7, "cfg-1", "sk-1")
	require.NoError(t, err)
	skill.CatalogID = "cat-migrated"
	skill.BundleRef = installRef
	skill.BundleSHA256 = sha
	skill.Status = types.SkillStatusRemoving
	require.NoError(t, fx.skillRepo.UpdateSkill(ctx, skill))

	readBack, err := fx.svc.loadCatalogArchive(ctx, 7, "cat-migrated")
	require.NoError(t, err)
	require.Equal(t, archive, readBack)
	cat, err := fx.skillRepo.GetCatalog(ctx, 7, "cat-migrated")
	require.NoError(t, err)
	require.NotEmpty(t, cat.BundleRef)
	require.NotEqual(t, installRef, cat.BundleRef)

	require.NoError(t, fx.svc.finishRemoval(ctx, 7, "cfg-1", "sk-1", false))
	require.Contains(t, fx.deletedBundles, installRef)
	require.NotContains(t, fx.deletedBundles, cat.BundleRef)

	result, err := fx.svc.InstallCatalogToConfigs(ctx, 7, "cat-migrated", []string{"cfg-1"})
	require.NoError(t, err)
	require.NotEmpty(t, result.Installs["cfg-1"])
}

func TestInstallCatalogToConfigsReportsPartialFailure(t *testing.T) {
	fx := newInstallFixture(t)
	ctx := context.Background()
	archive := zipBundle(t, map[string]string{
		"SKILL.md":           validSkillMD,
		"scripts/extract.py": "print('hi')\n",
	})
	cat, err := fx.svc.RegisterCatalogFromArchive(ctx, 7, archive)
	require.NoError(t, err)
	require.NotNil(t, cat)

	result, err := fx.svc.InstallCatalogToConfigs(ctx, 7, cat.ID, []string{"cfg-1", "missing"})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Installs, "cfg-1")
	require.Equal(t, "sandbox config not found", result.Errors["missing"])
}
