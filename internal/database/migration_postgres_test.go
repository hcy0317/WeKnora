package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newEphemeralPostgresSchema(t *testing.T) (string, *sql.DB) {
	t.Helper()
	baseDSN := os.Getenv("WEKNORA_TEST_POSTGRES_DSN")
	if baseDSN == "" || os.Getenv("WEKNORA_TEST_POSTGRES_EPHEMERAL") != "1" {
		t.Skip("explicit ephemeral PostgreSQL test environment is required")
	}
	base, err := sql.Open("postgres", baseDSN)
	require.NoError(t, err)
	require.NoError(t, base.Ping())
	schema := fmt.Sprintf("g004_migration_%d", time.Now().UnixNano())
	require.True(t, strings.HasPrefix(schema, "g004_migration_"))
	_, err = base.Exec("CREATE SCHEMA " + schema)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = base.Exec("DROP SCHEMA " + schema + " CASCADE")
		_ = base.Close()
	})
	separator := "?"
	if strings.Contains(baseDSN, "?") {
		separator = "&"
	}
	dsn := baseDSN + separator + "search_path=" + schema + "&options=-c%20app.skip_embedding=true"
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	require.NoError(t, db.Ping())
	t.Cleanup(func() { _ = db.Close() })
	return dsn, db
}

func useRepositoryRoot(t *testing.T) {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previous, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(root))
	t.Cleanup(func() { _ = os.Chdir(previous) })
}

const migrationSpanDDL = `CREATE TABLE knowledge_processing_spans (
	id BIGSERIAL PRIMARY KEY, knowledge_id VARCHAR(64) NOT NULL,
	attempt INT NOT NULL DEFAULT 1, span_id VARCHAR(64) NOT NULL,
	name VARCHAR(64) NOT NULL, kind VARCHAR(16) NOT NULL,
	status VARCHAR(16) NOT NULL, created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
	UNIQUE (knowledge_id, attempt, span_id));`

const migrationTaskPendingOpsDDL = `CREATE TABLE task_pending_ops (
	id BIGSERIAL PRIMARY KEY,
	tenant_id BIGINT NOT NULL,
	task_type VARCHAR(64) NOT NULL,
	scope VARCHAR(32) NOT NULL,
	scope_id VARCHAR(64) NOT NULL,
	op VARCHAR(32) NOT NULL,
	dedup_key VARCHAR(128) NOT NULL DEFAULT '',
	payload JSONB NOT NULL DEFAULT '{}'::JSONB,
	fail_count INT NOT NULL DEFAULT 0,
	enqueued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	claimed_at TIMESTAMPTZ);`

func createVersion80Schema(t *testing.T, db *sql.DB) {
	t.Helper()
	require.NoError(t, execSQL(db, migrationSpanDDL))
	require.NoError(t, execSQL(db, migrationTaskPendingOpsDDL))
}

func setMigrationVersion(t *testing.T, db *sql.DB, version uint, dirty bool) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE schema_migrations (version BIGINT NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL)`)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO schema_migrations(version, dirty) VALUES ($1, $2)`, version, dirty)
	require.NoError(t, err)
}

func TestPostgresMigration80To91AndIdempotentSQL(t *testing.T) {
	useRepositoryRoot(t)
	dsn, db := newEphemeralPostgresSchema(t)
	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(80))
	_, _ = m.Close()
	require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{}))

	var version uint
	var dirty bool
	require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
	assert.Equal(t, uint(91), version)
	assert.False(t, dirty)
	var definition string
	require.NoError(t, db.QueryRow(`SELECT indexdef FROM pg_indexes
		WHERE schemaname = current_schema() AND indexname = 'idx_knowledge_processing_spans_root_attempt_unique'`).Scan(&definition))
	assert.Contains(t, definition, "UNIQUE INDEX")
	assert.Contains(t, definition, "WHERE ((kind)::text = 'root'::text)")
	var manifestColumns int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'question_generation_manifests'
		AND column_name IN ('identity_version', 'generation_key', 'task_id', 'vector_store_id',
		'embedding_model_id', 'embedding_dimension', 'knowledge_type', 'effective_engines',
		'state', 'questions', 'index_entries', 'desired_source_ids', 'abandoned_source_ids')`).Scan(&manifestColumns))
	assert.Equal(t, 13, manifestColumns)
	var checkpointTables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name IN
		('wiki_ingest_work_units', 'wiki_taxonomy_plans', 'wiki_slug_applications', 'wiki_slug_contribution_markers')`).Scan(&checkpointTables))
	assert.Equal(t, 4, checkpointTables)
	var fragmentTables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = 'wiki_generation_fragments'`).Scan(&fragmentTables))
	assert.Equal(t, 1, fragmentTables)
	var completionOutboxTables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = 'knowledge_completion_outbox'`).Scan(&completionOutboxTables))
	assert.Equal(t, 1, completionOutboxTables)
	require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{}), "second migration run must be a no-op")

	up, err := os.ReadFile("migrations/versioned/000085_knowledge_processing_root_attempt_unique.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("migrations/versioned/000085_knowledge_processing_root_attempt_unique.down.sql")
	require.NoError(t, err)
	require.NoError(t, execSQL(db, string(down)))
	require.NoError(t, execSQL(db, string(down)))
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))
}

func TestPostgresMigration88WikiCheckpointRollbackIsIdempotent(t *testing.T) {
	useRepositoryRoot(t)
	_, db := newEphemeralPostgresSchema(t)
	up, err := os.ReadFile("migrations/versioned/000088_wiki_ingest_checkpoints.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("migrations/versioned/000088_wiki_ingest_checkpoints.down.sql")
	require.NoError(t, err)
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(down)))
	require.NoError(t, execSQL(db, string(down)))
	var tables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name LIKE 'wiki_%'`).Scan(&tables))
	assert.Zero(t, tables)
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))
}

func TestPostgresMigration87QuestionGenerationManifestRollbackIsIdempotent(t *testing.T) {
	useRepositoryRoot(t)
	_, db := newEphemeralPostgresSchema(t)
	up, err := os.ReadFile("migrations/versioned/000087_question_generation_manifests.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("migrations/versioned/000087_question_generation_manifests.down.sql")
	require.NoError(t, err)

	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(down)))
	require.NoError(t, execSQL(db, string(down)))
	var tables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = 'question_generation_manifests'`).Scan(&tables))
	assert.Zero(t, tables)
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))
}

func TestPostgresMigration86ClaimOwnershipUpDownIsIdempotent(t *testing.T) {
	useRepositoryRoot(t)
	_, db := newEphemeralPostgresSchema(t)
	require.NoError(t, execSQL(db, migrationTaskPendingOpsDDL))
	require.NoError(t, execSQL(db, `INSERT INTO task_pending_ops
		(tenant_id, task_type, scope, scope_id, op, claimed_at) VALUES
		(1, 'wiki:ingest', 'knowledge_base', 'kb-existing', 'upsert', CURRENT_TIMESTAMP)`))

	up, err := os.ReadFile("migrations/versioned/000086_task_pending_op_claim_ownership.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("migrations/versioned/000086_task_pending_op_claim_ownership.down.sql")
	require.NoError(t, err)
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))

	var token, taskID, heartbeat bool
	require.NoError(t, db.QueryRow(`SELECT
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'task_pending_ops' AND column_name = 'claim_token'),
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'task_pending_ops' AND column_name = 'claimed_by_task_id'),
		EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'task_pending_ops' AND column_name = 'claim_heartbeat_at')`).Scan(&token, &taskID, &heartbeat))
	assert.True(t, token)
	assert.True(t, taskID)
	assert.True(t, heartbeat)
	var legacyToken *string
	require.NoError(t, db.QueryRow(`SELECT claim_token FROM task_pending_ops WHERE scope_id = 'kb-existing'`).Scan(&legacyToken))
	assert.Nil(t, legacyToken, "migration must preserve pre-existing claims as tokenless legacy rows")
	var indexes int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname = current_schema() AND indexname = 'idx_task_pending_ops_claim_heartbeat'`).Scan(&indexes))
	assert.Equal(t, 1, indexes)

	require.NoError(t, execSQL(db, string(down)))
	require.NoError(t, execSQL(db, string(down)))
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'task_pending_ops'
		AND column_name IN ('claim_token', 'claimed_by_task_id', 'claim_heartbeat_at')`).Scan(&indexes))
	assert.Zero(t, indexes)
	require.NoError(t, execSQL(db, string(up)))
	require.NoError(t, execSQL(db, string(up)))
}

func TestPostgresMigration86RejectsMissingTaskPendingOps(t *testing.T) {
	useRepositoryRoot(t)
	_, db := newEphemeralPostgresSchema(t)
	up, err := os.ReadFile("migrations/versioned/000086_task_pending_op_claim_ownership.up.sql")
	require.NoError(t, err)
	err = execSQL(db, string(up))
	require.ErrorContains(t, err, `relation "task_pending_ops" does not exist`)
}

func TestPostgresMigration85To86To85To86(t *testing.T) {
	useRepositoryRoot(t)
	dsn, db := newEphemeralPostgresSchema(t)
	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = m.Close() })
	require.NoError(t, m.Migrate(85))
	require.NoError(t, execSQL(db, `INSERT INTO task_pending_ops
		(tenant_id, task_type, scope, scope_id, op, claimed_at) VALUES
		(1, 'wiki:ingest', 'knowledge_base', 'kb-existing', 'upsert', CURRENT_TIMESTAMP)`))

	require.NoError(t, m.Migrate(86))
	assertClaimOwnershipSchema(t, db, true)
	var legacyToken *string
	require.NoError(t, db.QueryRow(`SELECT claim_token FROM task_pending_ops WHERE scope_id = 'kb-existing'`).Scan(&legacyToken))
	assert.Nil(t, legacyToken, "migration must preserve pre-existing claims as tokenless legacy rows")

	require.NoError(t, m.Migrate(85))
	assertClaimOwnershipSchema(t, db, false)

	require.NoError(t, m.Migrate(86))
	assertClaimOwnershipSchema(t, db, true)
	require.NoError(t, db.QueryRow(`SELECT claim_token FROM task_pending_ops WHERE scope_id = 'kb-existing'`).Scan(&legacyToken))
	assert.Nil(t, legacyToken, "re-upgrade must not synthesize ownership for legacy claims")
}

func assertClaimOwnershipSchema(t *testing.T, db *sql.DB, want bool) {
	t.Helper()
	var columns int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'task_pending_ops'
		AND column_name IN ('claim_token', 'claimed_by_task_id', 'claim_heartbeat_at')`).Scan(&columns))
	if want {
		assert.Equal(t, 3, columns)
	} else {
		assert.Zero(t, columns)
	}
	var indexes int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname = current_schema() AND indexname = 'idx_task_pending_ops_claim_heartbeat'`).Scan(&indexes))
	if want {
		assert.Equal(t, 1, indexes)
		var definition string
		require.NoError(t, db.QueryRow(`SELECT indexdef FROM pg_indexes
			WHERE schemaname = current_schema() AND indexname = 'idx_task_pending_ops_claim_heartbeat'`).Scan(&definition))
		assert.Contains(t, definition, "(task_type, scope, scope_id, claim_heartbeat_at)")
		assert.Contains(t, definition, "WHERE (claimed_at IS NOT NULL)")
	} else {
		assert.Zero(t, indexes)
	}
}

func TestPostgresMigration85DirtyRecoversIdempotently(t *testing.T) {
	useRepositoryRoot(t)
	dsn, db := newEphemeralPostgresSchema(t)
	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(84))
	_, _ = m.Close()
	up, err := os.ReadFile("migrations/versioned/000085_knowledge_processing_root_attempt_unique.up.sql")
	require.NoError(t, err)
	require.NoError(t, execSQL(db, string(up)), "simulate migration SQL completing before dirty marker recovery")
	_, err = db.Exec(`UPDATE schema_migrations SET version = 85, dirty = true`)
	require.NoError(t, err)

	require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{AutoRecoverDirty: true}))
	var version uint
	var dirty bool
	require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
	assert.Equal(t, uint(89), version)
	assert.False(t, dirty)
	var indexes int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM pg_indexes
		WHERE schemaname = current_schema() AND indexname = 'idx_knowledge_processing_spans_root_attempt_unique'`).Scan(&indexes))
	assert.Equal(t, 1, indexes)
}

func TestPostgresMigrationGatePreservesDuplicateHistory(t *testing.T) {
	useRepositoryRoot(t)
	dsn, db := newEphemeralPostgresSchema(t)
	createVersion80Schema(t, db)
	setMigrationVersion(t, db, 80, false)
	_, err := db.Exec(`INSERT INTO knowledge_processing_spans
		(knowledge_id, attempt, span_id, name, kind, status) VALUES
		('duplicate-kid', 3, 'root-a', 'knowledge_processing', 'root', 'failed'),
		('duplicate-kid', 3, 'root-b', 'knowledge_processing', 'root', 'running')`)
	require.NoError(t, err)

	err = RunMigrationsWithOptions(dsn, MigrationOptions{})
	require.ErrorIs(t, err, ErrMigrationGate)
	var rows int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM knowledge_processing_spans`).Scan(&rows))
	assert.Equal(t, 2, rows)
	var version uint
	var dirty bool
	require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
	assert.Equal(t, uint(80), version)
	assert.False(t, dirty)
}

func TestPostgresMigrationGateDriftMatrix(t *testing.T) {
	useRepositoryRoot(t)
	t.Run("version80 relation missing", func(t *testing.T) {
		dsn, db := newEphemeralPostgresSchema(t)
		require.NoError(t, execSQL(db, migrationTaskPendingOpsDDL))
		setMigrationVersion(t, db, 80, false)
		err := RunMigrationsWithOptions(dsn, MigrationOptions{})
		require.ErrorIs(t, err, ErrMigrationGate)
		var version uint
		var dirty bool
		require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
		assert.Equal(t, uint(80), version)
		assert.False(t, dirty)
	})

	t.Run("pre55 relation exists", func(t *testing.T) {
		dsn, db := newEphemeralPostgresSchema(t)
		createVersion80Schema(t, db)
		setMigrationVersion(t, db, 54, false)
		err := RunMigrationsWithOptions(dsn, MigrationOptions{})
		require.ErrorIs(t, err, ErrMigrationGate)
		var version uint
		require.NoError(t, db.QueryRow(`SELECT version FROM schema_migrations`).Scan(&version))
		assert.Equal(t, uint(54), version)
	})

	t.Run("dirty rejected when recovery disabled", func(t *testing.T) {
		dsn, db := newEphemeralPostgresSchema(t)
		createVersion80Schema(t, db)
		setMigrationVersion(t, db, 80, true)
		err := RunMigrationsWithOptions(dsn, MigrationOptions{AutoRecoverDirty: false})
		require.Error(t, err)
		var version uint
		var dirty bool
		require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
		assert.Equal(t, uint(80), version)
		assert.True(t, dirty)
	})
}

func TestPostgresFreshAndPre55MigrateTo89(t *testing.T) {
	useRepositoryRoot(t)
	t.Run("fresh", func(t *testing.T) {
		dsn, db := newEphemeralPostgresSchema(t)
		require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{}))
		var version uint
		require.NoError(t, db.QueryRow(`SELECT version FROM schema_migrations`).Scan(&version))
		assert.Equal(t, uint(89), version)
	})

	t.Run("pre55", func(t *testing.T) {
		dsn, db := newEphemeralPostgresSchema(t)
		m, err := migrate.New("file://migrations/versioned", dsn)
		require.NoError(t, err)
		require.NoError(t, m.Migrate(54))
		_, _ = m.Close()
		var relation *string
		require.NoError(t, db.QueryRow(`SELECT to_regclass('knowledge_processing_spans')`).Scan(&relation))
		assert.Nil(t, relation)
		require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{}))
		var version uint
		require.NoError(t, db.QueryRow(`SELECT version FROM schema_migrations`).Scan(&version))
		assert.Equal(t, uint(89), version)
	})
}

func TestPostgresLegacyLocalVersion85ReplaysUpstreamMigrations(t *testing.T) {
	useRepositoryRoot(t)
	dsn, db := newEphemeralPostgresSchema(t)
	m, err := migrate.New("file://migrations/versioned", dsn)
	require.NoError(t, err)
	require.NoError(t, m.Migrate(80))
	_, _ = m.Close()

	for _, migration := range []string{
		"migrations/versioned/000085_knowledge_processing_root_attempt_unique.up.sql",
		"migrations/versioned/000086_task_pending_op_claim_ownership.up.sql",
		"migrations/versioned/000087_question_generation_manifests.up.sql",
		"migrations/versioned/000088_wiki_ingest_checkpoints.up.sql",
		"migrations/versioned/000089_wiki_canonical_identities.up.sql",
	} {
		contents, readErr := os.ReadFile(migration)
		require.NoError(t, readErr)
		require.NoError(t, execSQL(db, string(contents)))
	}
	_, err = db.Exec(`UPDATE schema_migrations SET version = 85, dirty = false`)
	require.NoError(t, err)

	var upstreamObjects int
	require.NoError(t, db.QueryRow(`SELECT
		(EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'messages' AND column_name = 'artifacts'))::int +
		(EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'tenant_sandbox_configs'))::int +
		(EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'sessions' AND column_name = 'sandbox_config_id'))::int +
		(EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'memory_subjects'))::int`).Scan(&upstreamObjects))
	require.Zero(t, upstreamObjects, "fixture must represent the old local v85 history")

	require.NoError(t, RunMigrationsWithOptions(dsn, MigrationOptions{}))
	var version uint
	var dirty bool
	require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
	require.Equal(t, uint(89), version)
	require.False(t, dirty)
	require.NoError(t, db.QueryRow(`SELECT
		(EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'messages' AND column_name = 'artifacts'))::int +
		(EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'tenant_sandbox_configs'))::int +
		(EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = 'sessions' AND column_name = 'sandbox_config_id'))::int +
		(EXISTS (SELECT 1 FROM information_schema.tables
			WHERE table_schema = current_schema() AND table_name = 'memory_subjects'))::int`).Scan(&upstreamObjects))
	require.Equal(t, 4, upstreamObjects)
}

func execSQL(db *sql.DB, statement string) error {
	_, err := db.ExecContext(context.Background(), statement)
	return err
}
