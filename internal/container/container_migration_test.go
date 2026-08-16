package container

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/Tencent/WeKnora/internal/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestMigrationGateErrorIsRecognizedAsFatal(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", database.ErrMigrationGate)
	fatalErr := fatalMigrationStartupError(err)
	assert.Error(t, fatalErr)
	assert.True(t, errors.Is(fatalErr, database.ErrMigrationGate))
	unsafeErr := fatalMigrationStartupError(fmt.Errorf("wrapped: %w", database.ErrMigrationUnsafe))
	assert.Error(t, unsafeErr)
	assert.True(t, errors.Is(unsafeErr, database.ErrMigrationUnsafe))
	assert.NoError(t, fatalMigrationStartupError(errors.New("ordinary externally managed migration failure")))
}

func TestInitDatabaseRunsStartupMaintenanceWhenMigrationsAreExternal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "external-migration.db")
	seed, err := gorm.Open(sqlite.Open(dbPath), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, seed.Exec(`CREATE TABLE knowledge_bases (
		id TEXT PRIMARY KEY,
		storage_provider_config TEXT
	)`).Error)
	require.NoError(t, seed.Exec(
		`INSERT INTO knowledge_bases(id, storage_provider_config) VALUES (?, ?)`,
		"kb-external", `{"provider":"__pending_env__"}`,
	).Error)
	seedSQL, err := seed.DB()
	require.NoError(t, err)
	require.NoError(t, seedSQL.Close())

	t.Setenv("DB_DRIVER", "sqlite")
	t.Setenv("DB_PATH", dbPath)
	t.Setenv("AUTO_MIGRATE", "false")
	t.Setenv("STORAGE_TYPE", "local")
	t.Setenv("REDIS_ADDR", "test-distributed-mode")

	db, err := initDatabase(nil)
	require.NoError(t, err)
	var providerConfig string
	require.NoError(t, db.Raw(
		`SELECT storage_provider_config FROM knowledge_bases WHERE id = ?`, "kb-external",
	).Scan(&providerConfig).Error)
	assert.JSONEq(t, `{"provider":"local"}`, providerConfig)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
}
