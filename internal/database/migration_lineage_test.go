package database

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const expectedPostgresMigrationVersion = 127

func TestMigrationLineageMigrationsHaveUniqueForwardVersions(t *testing.T) {
	repoRoot := sqliteRepoRoot(t)
	checks := []struct {
		name        string
		directory   string
		final       int
		newVersions []int
	}{
		{
			name:      "postgres",
			directory: filepath.Join(repoRoot, "migrations", "versioned"),
			final:     127,
			newVersions: func() []int {
				versions := make([]int, 0, 28)
				for version := 100; version <= 127; version++ {
					versions = append(versions, version)
				}
				return versions
			}(),
		},
		{
			name:      "sqlite",
			directory: filepath.Join(repoRoot, "migrations", "sqlite"),
			final:     46,
			newVersions: func() []int {
				versions := make([]int, 0, 22)
				for version := 25; version <= 46; version++ {
					versions = append(versions, version)
				}
				return versions
			}(),
		},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			entries, err := os.ReadDir(check.directory)
			require.NoError(t, err)

			versions := make(map[int]string)
			maximum := 0
			for _, entry := range entries {
				if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
					continue
				}
				prefix, _, ok := strings.Cut(entry.Name(), "_")
				require.Truef(t, ok, "migration filename %q must contain a numeric version", entry.Name())
				version, err := strconv.Atoi(prefix)
				require.NoError(t, err, "migration filename %q must have a numeric version", entry.Name())
				require.Emptyf(t, versions[version], "duplicate %s migration version %d", check.name, version)
				versions[version] = entry.Name()
				if version > maximum {
					maximum = version
				}
				down := strings.TrimSuffix(entry.Name(), ".up.sql") + ".down.sql"
				require.FileExists(t, filepath.Join(check.directory, down), "migration %q needs a matching down file", entry.Name())
			}

			require.Equal(t, check.final, maximum, "%s must include the complete forward-compatible migration range", check.name)
			for _, version := range check.newVersions {
				require.NotEmptyf(t, versions[version], "%s migration version %d must be present", check.name, version)
			}
		})
	}

	require.Equal(t, 46, expectedSQLiteMigrationVersion)
}
