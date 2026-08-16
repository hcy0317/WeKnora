package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	sqlite3migrate "github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

var (
	migrationStateMu        sync.RWMutex
	currentMigrationVersion uint
	currentMigrationDirty   bool
	migrationVersionSet     bool
	currentMigrationError   string
)

// ErrMigrationGate identifies pre-migration safety failures that must prevent
// application startup rather than falling back to externally managed schema.
var ErrMigrationGate = errors.New("postgres migration safety gate rejected")

// ErrMigrationUnsafe identifies migration states where continuing application
// startup could run new code against a partial or incompatible schema.
var ErrMigrationUnsafe = errors.New("database migration left schema unsafe")

// CachedMigrationVersion returns the migration version captured at startup.
// Returns (version, dirty, ok). ok is false if the version was never captured.
//
// Note: when migrations fail mid-way, the cache may still be populated via a
// best-effort m.Version() call inside RunMigrationsWithOptions so the system
// info endpoint can surface the partial state. Check CachedMigrationError() to
// distinguish a clean version reading from a recorded-after-failure one.
func CachedMigrationVersion() (uint, bool, bool) {
	migrationStateMu.RLock()
	defer migrationStateMu.RUnlock()
	return currentMigrationVersion, currentMigrationDirty, migrationVersionSet
}

// CachedMigrationError returns the error message captured when the most recent
// migration attempt failed at startup. Empty string means migrations either
// succeeded or were never run.
func CachedMigrationError() string {
	migrationStateMu.RLock()
	defer migrationStateMu.RUnlock()
	return currentMigrationError
}

// setMigrationState records the latest known migration state. Unlike the old
// sync.Once-based setter, this is intentionally idempotent-overwrite so the
// failure path (which runs after Up() errored) can replace the pre-migration
// snapshot taken from the initial m.Version() call.
func setMigrationState(version uint, dirty bool, errMsg string, versionKnown bool) {
	migrationStateMu.Lock()
	defer migrationStateMu.Unlock()
	if versionKnown {
		currentMigrationVersion = version
		currentMigrationDirty = dirty
		migrationVersionSet = true
	}
	currentMigrationError = errMsg
}

// captureMigrationFailure best-effort queries m for the current version so the
// system info endpoint can show "N (failed)" instead of vanishing the row, and
// stores the human-readable error message. Always returns the original error.
func captureMigrationFailure(m *migrate.Migrate, err error) error {
	versionKnown := false
	var ver uint
	var dirty bool
	if m != nil {
		v, d, vErr := m.Version()
		if vErr == nil {
			versionKnown = true
			ver, dirty = v, d
		}
	}
	setMigrationState(ver, dirty, err.Error(), versionKnown)
	return err
}

// RunMigrations executes all pending database migrations
// This should be called during application startup
func RunMigrations(dsn string) error {
	return RunMigrationsWithOptions(dsn, MigrationOptions{AutoRecoverDirty: false})
}

// MigrationOptions configures migration behavior
type MigrationOptions struct {
	// AutoRecoverDirty when true, automatically attempts to recover from dirty state
	// by forcing to the previous version and retrying the migration
	AutoRecoverDirty bool

	// SQLiteDBPath is the raw filesystem path to the SQLite database file.
	// When set, the migrator opens the DB directly via sql.Open instead of
	// parsing a URL-based DSN, which avoids breakage when the path contains
	// spaces (e.g. macOS "Application Support").
	SQLiteDBPath string
}

const (
	knowledgeSpanMigrationVersion = 55
	rootAttemptUniqueVersion      = 85
	postgresCollisionBaseVersion  = 80
	sqliteCollisionBaseVersion    = 3
)

type migrationGateSnapshot struct {
	Version      uint
	VersionKnown bool
	Dirty        bool
}

type migrationGateProbe struct {
	RelationExists func(context.Context) (bool, error)
	DuplicateRoots func(context.Context) (int64, error)
}

type migrationUpRunner interface {
	Version() (uint, bool, error)
	Up() error
}

func logDirtyMigrationGate(ctx context.Context, version uint) {
	logger.GetLogger(ctx).WithFields(logger.Fields{
		"version": version, "dirty": true,
		"relation_exists": "not_checked", "decision": "reject_dirty",
		"duplicate_count": "not_checked",
	}).Error("postgres migration gate")
}

func runMigrationUpOnce(
	ctx context.Context,
	runner migrationUpRunner,
	gate func(context.Context, migrationGateSnapshot) error,
) error {
	version, dirty, err := runner.Version()
	versionKnown := err == nil
	if err != nil && err != migrate.ErrNilVersion {
		return fmt.Errorf("%w: read migration version before safety gate: %v", ErrMigrationUnsafe, err)
	}
	if gate != nil {
		if err := gate(ctx, migrationGateSnapshot{
			Version: version, VersionKnown: versionKnown, Dirty: dirty,
		}); err != nil {
			return err
		}
	}
	return runner.Up()
}

func runPostgresMigrationGate(
	ctx context.Context, snapshot migrationGateSnapshot, probe migrationGateProbe,
) error {
	versionField := any("nil")
	if snapshot.VersionKnown {
		versionField = snapshot.Version
	}
	if snapshot.Dirty {
		logDirtyMigrationGate(ctx, snapshot.Version)
		return fmt.Errorf("%w: migration version is dirty", ErrMigrationGate)
	}
	relationExists, err := probe.RelationExists(ctx)
	if err != nil {
		logger.GetLogger(ctx).WithFields(logger.Fields{
			"version": versionField, "dirty": snapshot.Dirty,
			"relation_exists": "unknown", "decision": "probe_error",
			"duplicate_count": "not_checked",
		}).Error("postgres migration gate")
		return fmt.Errorf("%w: relation probe: %v", ErrMigrationGate, err)
	}

	decision := "allow_without_duplicate_check"
	duplicateCount := "not_checked"
	logDecision := func() {
		logger.GetLogger(ctx).WithFields(logger.Fields{
			"version":         versionField,
			"dirty":           snapshot.Dirty,
			"relation_exists": relationExists,
			"decision":        decision,
			"duplicate_count": duplicateCount,
		}).Info("postgres migration gate")
	}

	if (!snapshot.VersionKnown || snapshot.Version < knowledgeSpanMigrationVersion) && relationExists {
		decision = "reject_schema_drift_early_relation"
		logDecision()
		return fmt.Errorf("%w: schema drift: knowledge_processing_spans exists before migration %d", ErrMigrationGate, knowledgeSpanMigrationVersion)
	}
	if snapshot.VersionKnown && snapshot.Version >= knowledgeSpanMigrationVersion && !relationExists {
		decision = "reject_schema_drift_missing_relation"
		logDecision()
		return fmt.Errorf("%w: schema drift: knowledge_processing_spans is missing", ErrMigrationGate)
	}
	if snapshot.VersionKnown && snapshot.Version >= rootAttemptUniqueVersion {
		decision = "allow_already_at_root_unique_version"
		logDecision()
		return nil
	}
	if relationExists {
		count, countErr := probe.DuplicateRoots(ctx)
		if countErr != nil {
			decision = "reject_duplicate_probe_error"
			logDecision()
			return fmt.Errorf("%w: duplicate root probe: %v", ErrMigrationGate, countErr)
		}
		duplicateCount = fmt.Sprintf("%d", count)
		if count > 0 {
			decision = "reject_duplicate_roots"
			logDecision()
			return fmt.Errorf("%w: migration 000085 blocked: found %d duplicate root attempt group(s)", ErrMigrationGate, count)
		}
		decision = "allow_no_duplicate_roots"
	}
	logDecision()
	return nil
}

func postgresMigrationProbe(db *sql.DB) migrationGateProbe {
	return migrationGateProbe{
		RelationExists: func(ctx context.Context) (bool, error) {
			var exists bool
			err := db.QueryRowContext(ctx,
				"SELECT to_regclass('knowledge_processing_spans') IS NOT NULL").Scan(&exists)
			return exists, err
		},
		DuplicateRoots: func(ctx context.Context) (int64, error) {
			var count int64
			err := db.QueryRowContext(ctx, `
				SELECT COUNT(*) FROM (
					SELECT knowledge_id, attempt
					FROM knowledge_processing_spans
					WHERE kind = 'root'
					GROUP BY knowledge_id, attempt
					HAVING COUNT(*) > 1
				) duplicate_roots`).Scan(&count)
			return count, err
		},
	}
}

func postgresRelationExists(ctx context.Context, db *sql.DB, relation string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", relation).Scan(&exists)
	return exists, err
}

func postgresColumnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = $2
		)`, table, column).Scan(&exists)
	return exists, err
}

func postgresCollisionMarkers(
	ctx context.Context, db *sql.DB, version uint,
) (legacyExists, upstreamExists bool, err error) {
	switch version {
	case 81:
		legacyExists, err = postgresRelationExists(
			ctx, db, "idx_knowledge_processing_spans_root_attempt_unique",
		)
		if err == nil {
			upstreamExists, err = postgresColumnExists(ctx, db, "messages", "artifacts")
		}
	case 82:
		legacyExists, err = postgresColumnExists(ctx, db, "task_pending_ops", "claim_token")
		if err == nil {
			upstreamExists, err = postgresRelationExists(ctx, db, "tenant_sandbox_configs")
		}
	case 83:
		legacyExists, err = postgresRelationExists(ctx, db, "question_generation_manifests")
		if err == nil {
			upstreamExists, err = postgresColumnExists(ctx, db, "sessions", "sandbox_config_id")
		}
	case 84:
		legacyExists, err = postgresRelationExists(ctx, db, "wiki_ingest_work_units")
		if err == nil {
			upstreamExists, err = postgresRelationExists(ctx, db, "memory_subjects")
		}
	case 85:
		legacyExists, err = postgresRelationExists(ctx, db, "wiki_canonical_identities")
		if err == nil {
			upstreamExists, err = postgresRelationExists(ctx, db, "memory_subjects")
		}
	}
	return legacyExists, upstreamExists, err
}

func reconcilePostgresMigrationCollision(
	ctx context.Context, m *migrate.Migrate, db *sql.DB, version uint, dirty bool,
) (uint, error) {
	if dirty || version < 81 || version > 85 {
		return version, nil
	}
	legacyExists, upstreamExists, err := postgresCollisionMarkers(ctx, db, version)
	if err != nil {
		return version, fmt.Errorf("inspect PostgreSQL migration collision at version %d: %w", version, err)
	}
	if !legacyExists || upstreamExists {
		return version, nil
	}

	logger.Warnf(ctx,
		"Detected legacy local PostgreSQL migration version %d; replaying collision range from %d",
		version, postgresCollisionBaseVersion+1)
	if err := m.Force(postgresCollisionBaseVersion); err != nil {
		return version, fmt.Errorf("rewind legacy PostgreSQL migration version %d to %d: %w",
			version, postgresCollisionBaseVersion, err)
	}
	return postgresCollisionBaseVersion, nil
}

func sqliteTableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?
		)`, table).Scan(&exists)
	return exists, err
}

func reconcileSQLiteMigrationCollision(
	ctx context.Context, m *migrate.Migrate, db *sql.DB, version uint, dirty bool,
) (uint, error) {
	if dirty || version < 4 || version > 6 {
		return version, nil
	}
	memoryExists, err := sqliteTableExists(ctx, db, "memory_subjects")
	if err != nil {
		return version, fmt.Errorf("inspect upstream SQLite memory migration: %w", err)
	}
	if memoryExists {
		return version, nil
	}

	legacyTable := map[uint]string{
		4: "question_generation_manifests",
		5: "wiki_ingest_work_units",
		6: "wiki_canonical_identities",
	}[version]
	legacyExists, err := sqliteTableExists(ctx, db, legacyTable)
	if err != nil {
		return version, fmt.Errorf("inspect legacy SQLite migration %d: %w", version, err)
	}
	if !legacyExists {
		return version, nil
	}

	logger.Warnf(ctx,
		"Detected legacy local SQLite migration version %d; replaying collision range from %d",
		version, sqliteCollisionBaseVersion+1)
	if err := m.Force(sqliteCollisionBaseVersion); err != nil {
		return version, fmt.Errorf("rewind legacy SQLite migration version %d to %d: %w",
			version, sqliteCollisionBaseVersion, err)
	}
	return sqliteCollisionBaseVersion, nil
}

// RunMigrationsWithOptions executes all pending database migrations with custom options
func RunMigrationsWithOptions(dsn string, opts MigrationOptions) error {
	ctx := context.Background()

	logger.Infof(ctx, "Starting database migration...")

	migrationsPath := "file://migrations/versioned"
	if strings.HasPrefix(dsn, "sqlite3://") {
		migrationsPath = "file://migrations/sqlite"
	}

	var m *migrate.Migrate
	var sqliteMigrationDB *sql.DB
	if opts.SQLiteDBPath != "" {
		sqlDB, err := sql.Open("sqlite3", opts.SQLiteDBPath)
		if err != nil {
			logger.Errorf(ctx, "Failed to open sqlite db for migration: %v", err)
			wrapped := fmt.Errorf("%w: failed to open sqlite db for migration: %v", ErrMigrationUnsafe, err)
			setMigrationState(0, false, wrapped.Error(), false)
			return wrapped
		}
		sqliteMigrationDB = sqlDB
		driver, err := sqlite3migrate.WithInstance(sqlDB, &sqlite3migrate.Config{})
		if err != nil {
			sqlDB.Close()
			logger.Errorf(ctx, "Failed to create sqlite3 migrate driver: %v", err)
			wrapped := fmt.Errorf("%w: failed to create sqlite3 migrate driver: %v", ErrMigrationUnsafe, err)
			setMigrationState(0, false, wrapped.Error(), false)
			return wrapped
		}
		m, err = migrate.NewWithDatabaseInstance(migrationsPath, "sqlite3", driver)
		if err != nil {
			logger.Errorf(ctx, "Failed to create migrate instance: %v", err)
			wrapped := fmt.Errorf("%w: failed to create migrate instance: %v", ErrMigrationUnsafe, err)
			setMigrationState(0, false, wrapped.Error(), false)
			return wrapped
		}
	} else {
		var err error
		m, err = migrate.New(migrationsPath, dsn)
		if err != nil {
			logger.Errorf(ctx, "Failed to create migrate instance: %v", err)
			wrapped := fmt.Errorf("%w: failed to create migrate instance: %v", ErrMigrationUnsafe, err)
			setMigrationState(0, false, wrapped.Error(), false)
			return wrapped
		}
	}
	defer m.Close()

	// Check current version and dirty state before migration
	oldVersion, oldDirty, versionErr := m.Version()
	if versionErr != nil && versionErr != migrate.ErrNilVersion {
		logger.Errorf(ctx, "Failed to get migration version: %v", versionErr)
		return captureMigrationFailure(m, fmt.Errorf("%w: failed to get migration version: %v", ErrMigrationUnsafe, versionErr))
	}

	if versionErr == migrate.ErrNilVersion {
		logger.Infof(ctx, "Database has no migration history, will start from version 0")
	} else {
		logger.Infof(ctx, "Current migration version: %d, dirty: %v", oldVersion, oldDirty)
	}
	initialVersion := oldVersion

	// If database is in dirty state, try to recover or return error
	if oldDirty {
		logger.Warnf(ctx, "Database is in dirty state at version %d", oldVersion)
		if opts.AutoRecoverDirty {
			logger.Infof(ctx, "AutoRecoverDirty is enabled, attempting recovery...")
			if err := recoverFromDirtyState(ctx, m, oldVersion); err != nil {
				return captureMigrationFailure(m, fmt.Errorf("%w: %v", ErrMigrationUnsafe, err))
			}
			// Update oldVersion after recovery
			oldVersion, oldDirty, versionErr = m.Version()
			if versionErr != nil && versionErr != migrate.ErrNilVersion {
				return captureMigrationFailure(m, fmt.Errorf("%w: failed to read migration version after dirty recovery: %v", ErrMigrationUnsafe, versionErr))
			}
			if oldDirty {
				logDirtyMigrationGate(ctx, oldVersion)
				return captureMigrationFailure(m, fmt.Errorf("%w: dirty recovery did not clear version %d", ErrMigrationUnsafe, oldVersion))
			}
		} else {
			if opts.SQLiteDBPath == "" && !strings.HasPrefix(dsn, "sqlite3://") {
				logDirtyMigrationGate(ctx, oldVersion)
			}
			// Calculate the version to force to (usually the previous version)
			forceVersion := int(oldVersion) - 1
			if oldVersion == 0 || forceVersion < 0 {
				forceVersion = 0
			}
			return captureMigrationFailure(m, fmt.Errorf(
				"%w: "+
					"database is in dirty state at version %d. This usually means a migration failed partway through. "+
					"To fix this:\n"+
					"1. Check if the migration partially applied changes and manually fix if needed\n"+
					"2. Use the force command to set the version to the last successful migration (usually %d):\n"+
					"   ./scripts/migrate.sh force %d\n"+
					"   Or if using make: make migrate-force version=%d\n"+
					"3. After fixing, restart the application to retry the migration\n"+
					"Or enable AutoRecoverDirty option to automatically retry",
				ErrMigrationUnsafe,
				oldVersion,
				forceVersion,
				forceVersion,
				forceVersion,
			))
		}
	}

	if versionErr == nil && sqliteMigrationDB != nil {
		reconciledVersion, err := reconcileSQLiteMigrationCollision(
			ctx, m, sqliteMigrationDB, oldVersion, oldDirty,
		)
		if err != nil {
			return captureMigrationFailure(m, fmt.Errorf("%w: %v", ErrMigrationUnsafe, err))
		}
		oldVersion = reconciledVersion
	}

	var gateDB *sql.DB
	if opts.SQLiteDBPath == "" && !strings.HasPrefix(dsn, "sqlite3://") {
		var err error
		gateDB, err = sql.Open("postgres", dsn)
		if err != nil {
			return captureMigrationFailure(m, fmt.Errorf("%w: open postgres migration gate connection: %v", ErrMigrationUnsafe, err))
		}
		defer gateDB.Close()
		if versionErr == nil {
			reconciledVersion, reconcileErr := reconcilePostgresMigrationCollision(
				ctx, m, gateDB, oldVersion, oldDirty,
			)
			if reconcileErr != nil {
				return captureMigrationFailure(m, fmt.Errorf("%w: %v", ErrMigrationUnsafe, reconcileErr))
			}
			oldVersion = reconciledVersion
		}
	}
	upOnce := func() error {
		var gate func(context.Context, migrationGateSnapshot) error
		if gateDB != nil {
			gate = func(ctx context.Context, snapshot migrationGateSnapshot) error {
				return runPostgresMigrationGate(ctx, snapshot, postgresMigrationProbe(gateDB))
			}
		}
		return runMigrationUpOnce(ctx, m, gate)
	}

	// Run all pending migrations
	logger.Infof(ctx, "Running pending migrations...")
	if err := upOnce(); err != nil && err != migrate.ErrNoChange {
		logger.Errorf(ctx, "Migration failed: %v", err)
		if errors.Is(err, ErrMigrationGate) {
			return captureMigrationFailure(m, err)
		}
		// Check if error is due to dirty state (in case it became dirty during migration)
		currentVersion, currentDirty, versionCheckErr := m.Version()
		if versionCheckErr == nil && currentDirty {
			logger.Warnf(ctx, "Migration caused dirty state at version %d", currentVersion)
			if opts.AutoRecoverDirty {
				logger.Infof(ctx, "Attempting to recover from dirty state...")
				// Try to recover and retry
				if recoverErr := recoverFromDirtyState(ctx, m, currentVersion); recoverErr != nil {
					return captureMigrationFailure(m, fmt.Errorf("%w: %v", ErrMigrationUnsafe, recoverErr))
				}
				// Retry migration after recovery
				logger.Infof(ctx, "Retrying migration after recovery...")
				if retryErr := upOnce(); retryErr != nil && retryErr != migrate.ErrNoChange {
					logger.Errorf(ctx, "Migration failed after recovery attempt: %v", retryErr)
					return captureMigrationFailure(m, fmt.Errorf("%w: migration failed after recovery attempt: %v", ErrMigrationUnsafe, retryErr))
				}
			} else {
				// Calculate the version to force to (usually the previous version)
				forceVersion := currentVersion - 1
				if currentVersion == 0 {
					forceVersion = 0
				}
				return captureMigrationFailure(m, fmt.Errorf(
					"%w: "+
						"migration failed and database is now in dirty state at version %d. "+
						"To fix this:\n"+
						"1. Check if the migration partially applied changes and manually fix if needed\n"+
						"2. Use the force command to set the version to the last successful migration (usually %d):\n"+
						"   ./scripts/migrate.sh force %d\n"+
						"   Or if using make: make migrate-force version=%d\n"+
						"3. After fixing, restart the application to retry the migration\n"+
						"Or enable AutoRecoverDirty option to automatically retry",
					ErrMigrationUnsafe,
					currentVersion,
					forceVersion,
					forceVersion,
					forceVersion,
				))
			}
		} else {
			return captureMigrationFailure(m, fmt.Errorf("%w: failed to run migrations: %v", ErrMigrationUnsafe, err))
		}
	}

	// Get current version after migration
	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		return captureMigrationFailure(m, fmt.Errorf("%w: failed to get migration version: %v", ErrMigrationUnsafe, err))
	}

	setMigrationState(version, dirty, "", true)

	if initialVersion != version {
		logger.Infof(ctx, "Database migrated from version %d to %d", initialVersion, version)
	} else {
		logger.Infof(ctx, "Database is up to date (version: %d)", version)
	}

	if dirty {
		return captureMigrationFailure(m, fmt.Errorf("%w: migration completed with dirty version %d", ErrMigrationUnsafe, version))
	}

	return nil
}

// recoverFromDirtyState attempts to recover from a dirty migration state
// by forcing to the previous version and allowing the migration to be retried
func recoverFromDirtyState(ctx context.Context, m *migrate.Migrate, dirtyVersion uint) error {
	// Special case: if dirty at version 0 (init migration), we cannot go back further
	// The only option is to force to version 0 and retry, but this requires the migration to be idempotent
	if dirtyVersion == 0 {
		logger.Warnf(ctx, "Database is in dirty state at version 0 (init migration). "+
			"This is the initial migration, cannot rollback further. "+
			"Will attempt to clear dirty flag and retry. "+
			"Note: This only works if the init migration uses IF NOT EXISTS clauses.")

		// Force to version -1 (no version) to allow re-running version 0
		// This effectively tells migrate that no migrations have been applied
		if err := m.Force(-1); err != nil {
			return fmt.Errorf(
				"failed to recover from dirty state at version 0. "+
					"Manual intervention required:\n"+
					"1. Check what was partially created in the database\n"+
					"2. Either drop all created objects and retry, or\n"+
					"3. Manually complete the migration and run: ./scripts/migrate.sh force 0\n"+
					"Error: %w", err)
		}

		logger.Infof(ctx, "Cleared migration state, will retry from version 0")
		return nil
	}

	forceVersion := int(dirtyVersion) - 1

	logger.Warnf(ctx, "Database is in dirty state at version %d, attempting auto-recovery by forcing to version %d",
		dirtyVersion, forceVersion)

	// Force to previous version to clear dirty state
	if err := m.Force(forceVersion); err != nil {
		return fmt.Errorf("failed to force migration version during recovery: %w", err)
	}

	logger.Infof(ctx, "Successfully forced migration to version %d, migration will be retried", forceVersion)
	return nil
}

// GetMigrationVersion returns the current migration version
func GetMigrationVersion() (uint, bool, error) {
	dbURL := fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=disable",
		os.Getenv("DB_USER"),
		os.Getenv("DB_PASSWORD"),
		os.Getenv("DB_HOST"),
		os.Getenv("DB_PORT"),
		os.Getenv("DB_NAME"),
	)

	migrationsPath := "file://migrations/versioned"

	m, err := migrate.New(migrationsPath, dbURL)
	if err != nil {
		return 0, false, fmt.Errorf("failed to create migrate instance: %w", err)
	}
	defer m.Close()

	version, dirty, err := m.Version()
	if err != nil {
		return 0, false, err
	}

	return version, dirty, nil
}
