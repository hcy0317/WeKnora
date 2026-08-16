package database

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteMigrationsIncludeAutoTagConfig(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	dbPath := filepath.Join(t.TempDir(), "migration.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))

	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	rows, err := db.Query("PRAGMA table_info(knowledge_bases)")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	found := false
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		if name == "auto_tag_config" {
			found = true
		}
	}
	require.NoError(t, rows.Err())
	require.True(t, found, "SQLite migrations must create knowledge_bases.auto_tag_config")
}

func TestSQLiteMigrationsIncludeQuestionGenerationManifestSnapshot(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	dbPath := filepath.Join(t.TempDir(), "question-manifest.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	required := map[string]bool{
		"identity_version": false, "generation_key": false, "task_id": false,
		"vector_store_id": false, "embedding_model_id": false, "embedding_dimension": false,
		"knowledge_type": false, "effective_engines": false, "state": false,
		"questions": false, "index_entries": false, "desired_source_ids": false,
		"abandoned_source_ids": false,
	}
	rows, err := db.Query("PRAGMA table_info(question_generation_manifests)")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		if _, ok := required[name]; ok {
			required[name] = true
		}
	}
	require.NoError(t, rows.Err())
	for column, found := range required {
		require.Truef(t, found, "question manifest snapshot column %s is required", column)
	}
}

func TestSQLiteQuestionGenerationManifestMigrationRollbackIsIdempotent(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	dbPath := filepath.Join(t.TempDir(), "question-manifest-rollback.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	up, err := os.ReadFile("migrations/sqlite/000005_question_generation_manifests.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("migrations/sqlite/000005_question_generation_manifests.down.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.NoError(t, err)
	var tables int
	require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'question_generation_manifests'`).Scan(&tables))
	require.Zero(t, tables)
	_, err = db.Exec(string(up))
	require.NoError(t, err)
	_, err = db.Exec(string(up))
	require.NoError(t, err)
}

func TestSQLiteMigrationsIncludeWikiIngestCheckpoints(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	dbPath := filepath.Join(t.TempDir(), "wiki-checkpoints.db")
	require.NoError(t, RunMigrationsWithOptions("sqlite3://unused", MigrationOptions{SQLiteDBPath: dbPath}))
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	for _, table := range []string{"wiki_ingest_work_units", "wiki_taxonomy_plans", "wiki_slug_applications", "wiki_slug_contribution_markers"} {
		var count int
		require.NoError(t, db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&count))
		require.Equalf(t, 1, count, "migration must create %s", table)
	}
	rows, err := db.Query("PRAGMA table_info(wiki_ingest_work_units)")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	foundDocumentKey := false
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue any
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		foundDocumentKey = foundDocumentKey || name == "source_document_key"
	}
	require.NoError(t, rows.Err())
	require.True(t, foundDocumentKey)
}

func TestSQLiteWikiCheckpointMigrationRollbackIsIdempotent(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	previousDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(repoRoot))
	t.Cleanup(func() { _ = os.Chdir(previousDir) })

	db, err := sql.Open("sqlite3", filepath.Join(t.TempDir(), "wiki-checkpoint-rollback.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	up, err := os.ReadFile("migrations/sqlite/000006_wiki_ingest_checkpoints.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("migrations/sqlite/000006_wiki_ingest_checkpoints.down.sql")
	require.NoError(t, err)
	_, err = db.Exec(string(up))
	require.NoError(t, err)
	_, err = db.Exec(string(up))
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.NoError(t, err)
	_, err = db.Exec(string(down))
	require.NoError(t, err)
	_, err = db.Exec(string(up))
	require.NoError(t, err)
}

func TestSQLiteLegacyLocalVersion6ReplaysUpstreamMemory(t *testing.T) {
	useRepositoryRoot(t)
	dbPath := filepath.Join(t.TempDir(), "legacy-local-v6.db")
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE tenants (id INTEGER PRIMARY KEY);
		CREATE TABLE messages (id TEXT PRIMARY KEY);
		CREATE TABLE schema_migrations (version INTEGER NOT NULL PRIMARY KEY, dirty BOOLEAN NOT NULL);
		INSERT INTO schema_migrations(version, dirty) VALUES (6, 0);
	`)
	require.NoError(t, err)
	for _, migration := range []string{
		"migrations/sqlite/000005_question_generation_manifests.up.sql",
		"migrations/sqlite/000006_wiki_ingest_checkpoints.up.sql",
		"migrations/sqlite/000007_wiki_canonical_identities.up.sql",
	} {
		contents, readErr := os.ReadFile(migration)
		require.NoError(t, readErr)
		_, execErr := db.Exec(string(contents))
		require.NoError(t, execErr)
	}
	require.NoError(t, db.Close())

	require.NoError(t, RunMigrationsWithOptions(
		"sqlite3://unused",
		MigrationOptions{SQLiteDBPath: dbPath},
	))

	db, err = sql.Open("sqlite3", dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	var version uint
	var dirty bool
	require.NoError(t, db.QueryRow(`SELECT version, dirty FROM schema_migrations`).Scan(&version, &dirty))
	require.Equal(t, uint(7), version)
	require.False(t, dirty)

	for _, table := range []string{
		"memory_subjects",
		"question_generation_manifests",
		"wiki_ingest_work_units",
		"wiki_canonical_identities",
	} {
		var count int
		require.NoError(t, db.QueryRow(
			`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
		).Scan(&count))
		require.Equalf(t, 1, count, "legacy upgrade must preserve or create %s", table)
	}
	for _, column := range []struct {
		table string
		name  string
	}{
		{table: "tenants", name: "memory_config"},
		{table: "messages", name: "used_memories"},
	} {
		rows, queryErr := db.Query("PRAGMA table_info(" + column.table + ")")
		require.NoError(t, queryErr)
		found := false
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, columnType string
			var defaultValue any
			require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
			found = found || name == column.name
		}
		require.NoError(t, rows.Close())
		require.Truef(t, found, "legacy upgrade must add %s.%s", column.table, column.name)
	}
}
