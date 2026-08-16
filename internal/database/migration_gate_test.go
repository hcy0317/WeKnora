package database

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeMigrationUpRunner struct {
	snapshot migrationGateSnapshot
	calls    []string
	upCalls  int
	upErrors []error
}

func (f *fakeMigrationUpRunner) Version() (uint, bool, error) {
	f.calls = append(f.calls, "version")
	if !f.snapshot.VersionKnown {
		return 0, false, migrate.ErrNilVersion
	}
	return f.snapshot.Version, f.snapshot.Dirty, nil
}

func (f *fakeMigrationUpRunner) Up() error {
	f.calls = append(f.calls, "up")
	f.upCalls++
	if len(f.upErrors) >= f.upCalls {
		return f.upErrors[f.upCalls-1]
	}
	return nil
}

func TestPostgresMigrationGateMatrix(t *testing.T) {
	tests := []struct {
		name               string
		snapshot           migrationGateSnapshot
		relationExists     bool
		duplicateRoots     int64
		wantErr            string
		wantDuplicateProbe bool
	}{
		{name: "fresh missing relation", snapshot: migrationGateSnapshot{}, relationExists: false},
		{name: "pre55 missing relation", snapshot: migrationGateSnapshot{Version: 54, VersionKnown: true}, relationExists: false},
		{name: "version80 no duplicates", snapshot: migrationGateSnapshot{Version: 80, VersionKnown: true}, relationExists: true, wantDuplicateProbe: true},
		{name: "version80 duplicates", snapshot: migrationGateSnapshot{Version: 80, VersionKnown: true}, relationExists: true, duplicateRoots: 2, wantErr: "found 2 duplicate", wantDuplicateProbe: true},
		{name: "version84 relation exists", snapshot: migrationGateSnapshot{Version: 84, VersionKnown: true}, relationExists: true, wantDuplicateProbe: true},
		{name: "version85 relation exists", snapshot: migrationGateSnapshot{Version: 85, VersionKnown: true}, relationExists: true},
		{name: "version81 missing relation", snapshot: migrationGateSnapshot{Version: 81, VersionKnown: true}, relationExists: false, wantErr: "is missing"},
		{name: "fresh unexpected relation", snapshot: migrationGateSnapshot{}, relationExists: true, wantErr: "exists before migration"},
		{name: "pre55 unexpected relation", snapshot: migrationGateSnapshot{Version: 54, VersionKnown: true}, relationExists: true, wantErr: "exists before migration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			duplicateCalls := 0
			runner := &fakeMigrationUpRunner{snapshot: tt.snapshot}
			err := runMigrationUpOnce(context.Background(), runner, func(ctx context.Context, snapshot migrationGateSnapshot) error {
				return runPostgresMigrationGate(ctx, snapshot, migrationGateProbe{
					RelationExists: func(context.Context) (bool, error) {
						runner.calls = append(runner.calls, "relation")
						return tt.relationExists, nil
					},
					DuplicateRoots: func(context.Context) (int64, error) {
						runner.calls = append(runner.calls, "duplicates")
						duplicateCalls++
						return tt.duplicateRoots, nil
					},
				})
			})
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
			assert.Equal(t, tt.wantDuplicateProbe, duplicateCalls == 1)
			wantUpCalls := 1
			if tt.wantErr != "" {
				wantUpCalls = 0
			}
			assert.Equal(t, wantUpCalls, runner.upCalls)
			require.Equal(t, "version", runner.calls[0])
			if wantUpCalls == 1 {
				require.Equal(t, "up", runner.calls[len(runner.calls)-1])
			}
		})
	}
}

func TestPostgresMigrationGateRejectsDirtyBeforeProbes(t *testing.T) {
	relationCalls := 0
	duplicateCalls := 0
	runner := &fakeMigrationUpRunner{snapshot: migrationGateSnapshot{Version: 80, VersionKnown: true, Dirty: true}}
	err := runMigrationUpOnce(context.Background(), runner, func(ctx context.Context, snapshot migrationGateSnapshot) error {
		return runPostgresMigrationGate(ctx, snapshot, migrationGateProbe{
			RelationExists: func(context.Context) (bool, error) { relationCalls++; return true, nil },
			DuplicateRoots: func(context.Context) (int64, error) { duplicateCalls++; return 0, nil },
		})
	})
	require.ErrorIs(t, err, ErrMigrationGate)
	assert.Zero(t, relationCalls)
	assert.Zero(t, duplicateCalls)
	assert.Zero(t, runner.upCalls)
	assert.Equal(t, []string{"version"}, runner.calls)
}

func TestPostgresMigrationGateRelationProbeErrorIsFatal(t *testing.T) {
	err := runPostgresMigrationGate(context.Background(), migrationGateSnapshot{
		Version: 80, VersionKnown: true,
	}, migrationGateProbe{
		RelationExists: func(context.Context) (bool, error) { return false, errors.New("probe failed") },
		DuplicateRoots: func(context.Context) (int64, error) { return 0, nil },
	})
	require.ErrorIs(t, err, ErrMigrationGate)
	require.ErrorContains(t, err, "probe failed")
}

func TestMigrationRetryMustPassGateAgain(t *testing.T) {
	runner := &fakeMigrationUpRunner{
		snapshot: migrationGateSnapshot{Version: 80, VersionKnown: true},
		upErrors: []error{errors.New("injected migration interruption"), nil},
	}
	gateCalls := 0
	gate := func(context.Context, migrationGateSnapshot) error {
		gateCalls++
		runner.calls = append(runner.calls, "gate")
		return nil
	}
	require.Error(t, runMigrationUpOnce(context.Background(), runner, gate))
	// Simulate Force(80) plus the production clean Version re-read before retry.
	runner.snapshot = migrationGateSnapshot{Version: 80, VersionKnown: true, Dirty: false}
	require.NoError(t, runMigrationUpOnce(context.Background(), runner, gate))
	assert.Equal(t, 2, gateCalls)
	assert.Equal(t, 2, runner.upCalls)
	assert.Equal(t, []string{"version", "gate", "up", "version", "gate", "up"}, runner.calls)
}
