package repository

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupPostgresResourceTest(t *testing.T) (*gorm.DB, *gorm.DB) {
	t.Helper()
	dsn := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if dsn == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("explicit ephemeral PostgreSQL test environment is required")
	}
	base, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	require.NoError(t, err)
	baseSQL, err := base.DB()
	require.NoError(t, err)
	schema := fmt.Sprintf("resource_claim_%d", time.Now().UnixNano())
	require.NoError(t, base.Exec("CREATE SCHEMA "+schema).Error)
	t.Cleanup(func() {
		_ = base.Exec("DROP SCHEMA " + schema + " CASCADE").Error
		_ = baseSQL.Close()
	})
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	open := func() *gorm.DB {
		db, openErr := gorm.Open(postgres.Open(dsn+separator+"search_path="+schema), &gorm.Config{})
		require.NoError(t, openErr)
		sqlDB, sqlErr := db.DB()
		require.NoError(t, sqlErr)
		t.Cleanup(func() { _ = sqlDB.Close() })
		return db
	}
	first, second := open(), open()
	require.NoError(t, first.AutoMigrate(&types.StoredResource{}, &types.ResourceBinding{}))
	return first, second
}

func blockAfterResourceLock(t *testing.T, db *gorm.DB) (<-chan struct{}, func()) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	name := "test:block_after_resource_lock:" + uuid.NewString()
	require.NoError(t, db.Callback().Query().After("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != "resources" {
			return
		}
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
	}))
	t.Cleanup(func() { _ = db.Callback().Query().Remove(name) })
	return entered, func() { close(release) }
}

func assertPostgresResourceOutcome(t *testing.T, db *gorm.DB, id string, wantState string, wantBindings int64) {
	t.Helper()
	var resource types.StoredResource
	require.NoError(t, db.Unscoped().Where("id = ?", id).First(&resource).Error)
	require.Equal(t, wantState, resource.State)
	var bindings int64
	require.NoError(t, db.Model(&types.ResourceBinding{}).Where("resource_id = ?", id).Count(&bindings).Error)
	require.Equal(t, wantBindings, bindings)
}

func TestResourceRepositoryPostgresBindDeleteBarrierHasOneWinner(t *testing.T) {
	t.Run("bind wins", func(t *testing.T) {
		first, second := setupPostgresResourceTest(t)
		resource := &types.StoredResource{
			Handle: "BindWinnerAbCdEfGhIjKl", TenantID: 7, Provider: "local",
			PhysicalPath: "local://a", LocationHash: uuid.NewString(),
		}
		require.NoError(t, first.Create(resource).Error)
		bindRepo, deleteRepo := &resourceRepository{db: first}, &resourceRepository{db: second}
		entered, release := blockAfterResourceLock(t, first)

		bindDone := make(chan error, 1)
		go func() {
			active, err := bindRepo.CreateBindingIfActive(context.Background(), &types.ResourceBinding{
				ResourceID: resource.ID, TenantID: 7, OwnerType: "message", OwnerID: "msg", Relation: "artifact",
			})
			if err == nil && !active {
				err = fmt.Errorf("bind did not acquire active resource")
			}
			bindDone <- err
		}()
		<-entered
		deleteDone := make(chan struct {
			claim time.Time
			err   error
		}, 1)
		go func() {
			claim, err := deleteRepo.MarkDeletedIfUnbound(context.Background(), resource.ID)
			deleteDone <- struct {
				claim time.Time
				err   error
			}{claim, err}
		}()
		select {
		case result := <-deleteDone:
			t.Fatalf("delete escaped bind row lock: claim=%v err=%v", result.claim, result.err)
		case <-time.After(100 * time.Millisecond):
		}
		release()
		require.NoError(t, <-bindDone)
		result := <-deleteDone
		require.NoError(t, result.err)
		require.True(t, result.claim.IsZero(), "bound resource must reject deletion claim")
		assertPostgresResourceOutcome(t, first, resource.ID, types.ResourceStateActive, 1)
	})

	t.Run("delete wins", func(t *testing.T) {
		first, second := setupPostgresResourceTest(t)
		resource := &types.StoredResource{
			Handle: "DeleteWinAbCdEfGhIjKlM", TenantID: 7, Provider: "local",
			PhysicalPath: "local://b", LocationHash: uuid.NewString(),
		}
		require.NoError(t, first.Create(resource).Error)
		deleteRepo, bindRepo := &resourceRepository{db: first}, &resourceRepository{db: second}
		entered, release := blockAfterResourceLock(t, first)

		deleteDone := make(chan struct {
			claim time.Time
			err   error
		}, 1)
		go func() {
			claim, err := deleteRepo.MarkDeletedIfUnbound(context.Background(), resource.ID)
			deleteDone <- struct {
				claim time.Time
				err   error
			}{claim, err}
		}()
		<-entered
		bindDone := make(chan struct {
			active bool
			err    error
		}, 1)
		go func() {
			active, err := bindRepo.CreateBindingIfActive(context.Background(), &types.ResourceBinding{
				ResourceID: resource.ID, TenantID: 7, OwnerType: "message", OwnerID: "msg", Relation: "artifact",
			})
			bindDone <- struct {
				active bool
				err    error
			}{active, err}
		}()
		select {
		case result := <-bindDone:
			t.Fatalf("bind escaped delete row lock: active=%v err=%v", result.active, result.err)
		case <-time.After(100 * time.Millisecond):
		}
		release()
		deleted := <-deleteDone
		require.NoError(t, deleted.err)
		require.False(t, deleted.claim.IsZero())
		bound := <-bindDone
		require.NoError(t, bound.err)
		require.False(t, bound.active)
		assertPostgresResourceOutcome(t, first, resource.ID, types.ResourceStateDeleted, 0)
	})
}
