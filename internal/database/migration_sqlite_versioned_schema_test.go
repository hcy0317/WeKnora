package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// versionedSQLiteTables is the set of tables that SQLite migrations must
// create to stay in sync with the versioned (PostgreSQL) migrations:
// 000041 task queue, 000053 system settings, 000055 processing spans,
// 000063 knowledge multi-tags.
var versionedSQLiteTables = []string{
	"task_pending_ops",
	"task_dead_letters",
	"system_settings",
	"knowledge_processing_spans",
	"knowledge_tag_relations",
	"tenant_skills",
	"tenant_skill_snapshots",
	"tenant_user_env_vars",
	"tenant_skill_catalog",
	"tenant_skill_bundle_ref_claims",
}

// versionedSQLiteColumns maps each existing table to the columns that the
// versioned migrations add and the SQLite baseline was missing.
var versionedSQLiteColumns = map[string][]string{
	"tenants":                {"api_principal_config"},           // 000064
	"users":                  {"is_system_admin"},                // 000053
	"knowledges":             {"pending_subtasks_count"},         // 000056
	"messages":               {"attachments", "usage"},           // 000034, 000092
	"tenant_invitations":     {"token", "accepted_count"},        // 000054
	"embed_channels":         {"allow_memory"},                   // 000060
	"mcp_oauth_tokens":       {"principal_type", "principal_id"}, // 000064
	"tenant_skills":          {"install_session_id", "install_message_id", "envs", "catalog_id"},
	"tenant_skill_snapshots": {"planned_name"},
}

const expectedSQLiteMigrationVersion = 24

func TestSQLiteMigrationsCreateVersionedSchema(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	chdirAndRestore(t, repoRoot)

	dbPath := filepath.Join(t.TempDir(), "fresh.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))

	db := openSQLiteDB(t, dbPath)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)

	for _, table := range versionedSQLiteTables {
		require.Truef(t, sqliteTestTableExists(t, db, table), "SQLite migrations must create table %s", table)
	}
	for table, columns := range versionedSQLiteColumns {
		for _, column := range columns {
			require.Truef(
				t,
				sqliteColumnExists(t, db, table, column),
				"SQLite migrations must add column %s.%s",
				table,
				column,
			)
		}
	}

	assertSQLiteShareLinkInvitationsWork(t, db)
	assertSQLiteMCPOAuthPrincipalUpsertWorks(t, db)
	require.False(t, sqliteColumnExists(t, db, "knowledges", "tag_id"),
		"SQLite migrations must drop legacy knowledges.tag_id after multi-tag migration")
}

func TestSQLiteLegacyLocalVersion16ReceivesMessageUsage(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	v16Root := copySQLiteMigrationsThrough(t, repoRoot, 16)
	chdirAndRestore(t, v16Root)

	dbPath := filepath.Join(t.TempDir(), "legacy-local-v16.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))

	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db = openSQLiteDB(t, dbPath)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)
	require.True(t, sqliteColumnExists(t, db, "messages", "usage"))
}

func TestSQLiteVersion21BackfillsSkillCatalog(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	v21Root := copySQLiteMigrationsThrough(t, repoRoot, 21)
	chdirAndRestore(t, v21Root)

	dbPath := filepath.Join(t.TempDir(), "skill-catalog-v21.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db := openSQLiteDB(t, dbPath)
	_, err := db.Exec(`
		INSERT INTO tenant_skills (
			id, tenant_id, sandbox_config_id, name, bundle_ref, enabled, status, created_at, updated_at
		) VALUES
			('skill-old', 7, 'sandbox-a', 'demo', '', 1, 'installed', '2026-01-01', '2026-01-01'),
			('skill-new', 7, 'sandbox-b', 'demo',
			 'resource://aaaaaaaaaaaaaaaaaaaaaa', 1, 'installed', '2026-01-02', '2026-01-02')
	`)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db = openSQLiteDB(t, dbPath)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)
	var catalogID string
	require.NoError(t, db.QueryRow(
		"SELECT id FROM tenant_skill_catalog WHERE tenant_id = 7 AND name = 'demo'",
	).Scan(&catalogID))
	require.Equal(t, "skill-new", catalogID)
	var catalogBundleRef sql.NullString
	require.NoError(t, db.QueryRow(
		"SELECT bundle_ref FROM tenant_skill_catalog WHERE id = ?", catalogID,
	).Scan(&catalogBundleRef))
	require.False(t, catalogBundleRef.Valid,
		"backfill must not make the catalog co-own an installation bundle")
	var linked int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM tenant_skills WHERE catalog_id = ?", catalogID,
	).Scan(&linked))
	require.Equal(t, 2, linked)
}

func TestSQLiteVersion22ClearsPreviouslySharedCatalogBundleRef(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	v22Root := copySQLiteMigrationsThrough(t, repoRoot, 22)
	chdirAndRestore(t, v22Root)
	dbPath := filepath.Join(t.TempDir(), "skill-catalog-v22.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db := openSQLiteDB(t, dbPath)
	shared := "resource://shared-skill-bundle"
	sha := strings.Repeat("a", 64)
	_, err := db.Exec(`INSERT INTO tenant_skill_catalog
		(id, tenant_id, name, bundle_ref, bundle_sha256, created_at, updated_at)
		VALUES ('cat-1', 7, 'demo', ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, shared, sha)
	require.NoError(t, err)
	_, err = db.Exec(`INSERT INTO tenant_skills
		(id, tenant_id, sandbox_config_id, catalog_id, name, bundle_ref, bundle_sha256,
		 enabled, status, created_at, updated_at)
		VALUES ('skill-1', 7, 'cfg-1', 'cat-1', 'demo', ?, ?, 1, 'ready',
		 CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`, shared, sha)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db = openSQLiteDB(t, dbPath)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)
	var ref sql.NullString
	var gotSHA string
	require.NoError(t, db.QueryRow(
		"SELECT bundle_ref, bundle_sha256 FROM tenant_skill_catalog WHERE id = 'cat-1'",
	).Scan(&ref, &gotSHA))
	require.False(t, ref.Valid)
	require.Equal(t, sha, gotSHA)
}

func TestSQLiteCompatibilityVersion16WithUsageSkipsDuplicateDDL(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	v16Root := copySQLiteMigrationsThrough(t, repoRoot, 16)
	chdirAndRestore(t, v16Root)

	dbPath := filepath.Join(t.TempDir(), "compat-v16-with-usage.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	usageSQL, err := os.ReadFile(filepath.Join(repoRoot, "migrations", "sqlite", "000017_message_usage.up.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(usageSQL))
	require.NoError(t, err)
	require.NoError(t, db.Close())

	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db = openSQLiteDB(t, dbPath)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)
	require.True(t, sqliteColumnExists(t, db, "messages", "usage"))
}

func TestSQLiteTencentVersion12ReplaysForkMigrations(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	upstreamRoot := copySQLiteMigrationsThrough(t, repoRoot, 11)
	chdirAndRestore(t, upstreamRoot)

	dbPath := filepath.Join(t.TempDir(), "upstream-v12.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	usageSQL, err := os.ReadFile(filepath.Join(repoRoot, "migrations", "sqlite", "000017_message_usage.up.sql"))
	require.NoError(t, err)
	_, err = db.Exec(string(usageSQL))
	require.NoError(t, err)
	_, err = db.Exec("UPDATE schema_migrations SET version = 12, dirty = 0")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db = openSQLiteDB(t, dbPath)
	version, dirty := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, version)
	require.False(t, dirty)
	require.True(t, sqliteColumnExists(t, db, "messages", "usage"))
	for _, table := range []string{
		"question_generation_manifests",
		"wiki_ingest_work_units",
		"wiki_canonical_identities",
		"wiki_generation_fragments",
		"knowledge_completion_outbox",
	} {
		require.Truef(t, sqliteTestTableExists(t, db, table), "compatibility replay must create %s", table)
	}
}

func TestSQLiteMigrationsUpgradeV4PreservesData(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)

	// Build a legacy v4 migration root (000000_init .. 000004_memory) so we
	// can prove the new migrations upgrade an existing Lite database without
	// replaying the baseline.
	legacyRoot := copySQLiteMigrationsV4(t, repoRoot)
	chdirAndRestore(t, legacyRoot)

	dbPath := filepath.Join(t.TempDir(), "upgrade.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))

	db := openSQLiteDB(t, dbPath)
	versionBefore, dirtyBefore := sqliteMigrationState(t, db)
	require.Equal(t, 4, versionBefore)
	require.False(t, dirtyBefore)
	_, err := db.Exec("INSERT INTO tenants (name, business) VALUES (?, ?)", "upgrade-sentinel", "migration-test")
	require.NoError(t, err)
	_, err = db.Exec(
		"INSERT INTO knowledges (id, tenant_id, knowledge_base_id, type, title, source, tag_id) "+
			"VALUES (?, 1, ?, 'document', 'tagged-doc', 'manual', ?)",
		"legacy-knowledge-1", "legacy-kb-1", "legacy-tag-1",
	)
	require.NoError(t, err)

	// Run the full migration set from the repo root.
	chdirAndRestore(t, repoRoot)
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))

	db = openSQLiteDB(t, dbPath)
	versionAfter, dirtyAfter := sqliteMigrationState(t, db)
	require.Equal(t, expectedSQLiteMigrationVersion, versionAfter)
	require.False(t, dirtyAfter)

	for _, table := range versionedSQLiteTables {
		require.Truef(t, sqliteTestTableExists(t, db, table), "upgraded SQLite DB must have table %s", table)
	}
	for table, columns := range versionedSQLiteColumns {
		for _, column := range columns {
			require.Truef(
				t,
				sqliteColumnExists(t, db, table, column),
				"upgraded SQLite DB must have column %s.%s",
				table,
				column,
			)
		}
	}

	var sentinelName string
	require.NoError(t, db.QueryRow("SELECT name FROM tenants WHERE business = ?", "migration-test").Scan(&sentinelName))
	require.Equal(t, "upgrade-sentinel", sentinelName)

	var relationCount int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM knowledge_tag_relations WHERE knowledge_id = ? AND tag_id = ?",
		"legacy-knowledge-1", "legacy-tag-1",
	).Scan(&relationCount))
	require.Equal(t, 1, relationCount)
	require.False(t, sqliteColumnExists(t, db, "knowledges", "tag_id"))
}

func sqliteRepoRoot(t *testing.T) string {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	return repoRoot
}

func chdirAndRestore(t *testing.T, dir string) {
	t.Helper()
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(dir))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })
}

func openSQLiteDB(t *testing.T, dbPath string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func sqliteMigrationState(t *testing.T, db *sql.DB) (version int, dirty bool) {
	t.Helper()
	require.NoError(t, db.QueryRow("SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty))
	return version, dirty
}

func sqliteTestTableExists(t *testing.T, db *sql.DB, table string) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		table,
	).Scan(&n))
	return n == 1
}

func sqliteColumnExists(t *testing.T, db *sql.DB, table, column string) bool {
	t.Helper()
	var n int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?",
		table,
		column,
	).Scan(&n))
	return n == 1
}

func assertSQLiteShareLinkInvitationsWork(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec("INSERT INTO tenants (name, business) VALUES (?, ?)", "share-link-tenant", "share-link-test")
	require.NoError(t, err)

	expiresAt := "2099-01-01 00:00:00"
	shareLinkInsert := "INSERT INTO tenant_invitations " +
		"(tenant_id, invitee_user_id, token, role, status, expires_at) " +
		"VALUES (1, '', ?, 'member', 'pending', ?)"
	_, err = db.Exec(shareLinkInsert, "token-a", expiresAt)
	require.NoError(t, err)
	_, err = db.Exec(shareLinkInsert, "token-b", expiresAt)
	require.NoError(t, err)

	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM tenant_invitations WHERE tenant_id = 1 AND invitee_user_id = '' AND status = 'pending'",
	).Scan(&count))
	require.Equal(t, 2, count)
}

func assertSQLiteMCPOAuthPrincipalUpsertWorks(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(
		"INSERT INTO mcp_services (id, tenant_id, name, transport_type) VALUES (?, 1, 'svc', 'http')",
		"svc-migration-1",
	)
	require.NoError(t, err)

	tokenInsertPrefix := "INSERT INTO mcp_oauth_tokens " +
		"(id, tenant_id, user_id, service_id, principal_type, principal_id, access_token) "
	_, err = db.Exec(
		tokenInsertPrefix +
			"VALUES ('tok-1', 1, 'u1', 'svc-migration-1', 'web_user', 'u1', 'token-1')",
	)
	require.NoError(t, err)

	_, err = db.Exec(
		tokenInsertPrefix +
			"VALUES ('tok-2', 1, 'u1', 'svc-migration-1', 'web_user', 'u1', 'token-2') " +
			"ON CONFLICT(tenant_id, principal_type, principal_id, service_id) " +
			"DO UPDATE SET access_token = excluded.access_token",
	)
	require.NoError(t, err)

	var accessToken string
	require.NoError(t, db.QueryRow(
		"SELECT access_token FROM mcp_oauth_tokens "+
			"WHERE tenant_id = 1 AND principal_type = 'web_user' "+
			"AND principal_id = 'u1' AND service_id = 'svc-migration-1'",
	).Scan(&accessToken))
	require.Equal(t, "token-2", accessToken)

	var rowCount int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM mcp_oauth_tokens WHERE tenant_id = 1 AND service_id = 'svc-migration-1'",
	).Scan(&rowCount))
	require.Equal(t, 1, rowCount)
}

func copySQLiteMigrationsV4(t *testing.T, repoRoot string) string {
	t.Helper()
	dest := t.TempDir()
	srcDir := filepath.Join(repoRoot, "migrations", "sqlite")
	destDir := filepath.Join(dest, "migrations", "sqlite")
	require.NoError(t, os.MkdirAll(destDir, 0o755))

	legacy := []string{
		"000000_init.up.sql",
		"000001_remove_wiki_log.up.sql",
		"000002_knowledge_folder_path.up.sql",
		"000003_knowledge_base_auto_tag_config.up.sql",
		"000004_memory.up.sql",
	}
	for _, name := range legacy {
		data, err := os.ReadFile(filepath.Join(srcDir, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(destDir, name), data, 0o600))
	}
	return dest
}

func copySQLiteMigrationsThrough(t *testing.T, repoRoot string, maxVersion int) string {
	t.Helper()
	dest := t.TempDir()
	srcDir := filepath.Join(repoRoot, "migrations", "sqlite")
	destDir := filepath.Join(dest, "migrations", "sqlite")
	require.NoError(t, os.MkdirAll(destDir, 0o755))
	for version := 0; version <= maxVersion; version++ {
		matches, err := filepath.Glob(filepath.Join(srcDir, fmt.Sprintf("%06d_*.up.sql", version)))
		require.NoError(t, err)
		require.Lenf(t, matches, 1, "expected exactly one SQLite migration for version %d", version)
		data, err := os.ReadFile(matches[0])
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(destDir, filepath.Base(matches[0])), data, 0o600))
	}
	return dest
}
func copySQLiteMigrationsV3(t *testing.T, repoRoot string) string {
	t.Helper()
	dest := t.TempDir()
	srcDir := filepath.Join(repoRoot, "migrations", "sqlite")
	destDir := filepath.Join(dest, "migrations", "sqlite")
	require.NoError(t, os.MkdirAll(destDir, 0o755))

	legacy := []string{
		"000000_init.up.sql",
		"000001_remove_wiki_log.up.sql",
		"000002_knowledge_folder_path.up.sql",
		"000003_knowledge_base_auto_tag_config.up.sql",
	}
	for _, name := range legacy {
		data, err := os.ReadFile(filepath.Join(srcDir, name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(destDir, name), data, 0o600))
	}
	return dest
}
