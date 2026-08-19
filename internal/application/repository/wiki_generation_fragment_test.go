package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func setupWikiGenerationFragmentRepo(t *testing.T) (*wikiPageRepository, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.AutoMigrate(&types.WikiGenerationFragment{}))
	return NewWikiPageRepository(db).(*wikiPageRepository), db
}

func testWikiGenerationFragment() *types.WikiGenerationFragment {
	return &types.WikiGenerationFragment{
		FragmentID: "fragment-1", TenantID: 7, KnowledgeBaseID: "kb",
		WorkRevision: "work", Purpose: "wiki_summary", FragmentKey: "summary",
		PromptDigest: "prompt", ModelSnapshot: "model",
		State: types.WikiGenerationFragmentReady,
	}
}

func TestWikiGenerationFragmentReservationPersistsBudgetAcrossRedelivery(t *testing.T) {
	repo, _ := setupWikiGenerationFragmentRepo(t)
	ctx := context.Background()
	fragment := testWikiGenerationFragment()
	for attempt := 1; attempt <= 3; attempt++ {
		stored, granted, err := repo.ReserveWikiGenerationFragment(
			ctx, fragment, "call-"+string(rune('0'+attempt)), time.Now().Add(time.Minute), 3,
		)
		require.NoError(t, err)
		require.True(t, granted)
		require.Equal(t, attempt, stored.Attempts)
		require.NoError(t, repo.ReleaseWikiGenerationFragment(
			ctx, stored.FragmentID, stored.CallID, "typed transport failure", false,
		))
	}
	stored, granted, err := repo.ReserveWikiGenerationFragment(
		ctx, fragment, "call-4", time.Now().Add(time.Minute), 3,
	)
	require.NoError(t, err)
	require.False(t, granted)
	require.Equal(t, types.WikiGenerationFragmentTerminal, stored.State)
	require.Equal(t, 3, stored.Attempts)
}

func TestWikiGenerationFragmentOutputIsFencedAndReusable(t *testing.T) {
	repo, _ := setupWikiGenerationFragmentRepo(t)
	ctx := context.Background()
	fragment, granted, err := repo.ReserveWikiGenerationFragment(
		ctx, testWikiGenerationFragment(), "owner", time.Now().Add(time.Minute), 3,
	)
	require.NoError(t, err)
	require.True(t, granted)
	require.Error(t, repo.CompleteWikiGenerationFragment(ctx, fragment.FragmentID, "forged", "wrong"))
	require.NoError(t, repo.CompleteWikiGenerationFragment(ctx, fragment.FragmentID, "owner", "stable output"))

	replayed, granted, err := repo.ReserveWikiGenerationFragment(
		ctx, testWikiGenerationFragment(), "second-owner", time.Now().Add(time.Minute), 3,
	)
	require.NoError(t, err)
	require.False(t, granted)
	require.Equal(t, types.WikiGenerationFragmentGenerated, replayed.State)
	require.Equal(t, "stable output", replayed.Output)
	require.Equal(t, 1, replayed.Attempts)
}

func TestWikiGenerationFragmentExpiredCallingLeaseFailsClosed(t *testing.T) {
	repo, db := setupWikiGenerationFragmentRepo(t)
	ctx := context.Background()
	fragment := testWikiGenerationFragment()
	fragment.State = types.WikiGenerationFragmentCalling
	fragment.Attempts = 1
	fragment.CallID = "lost-owner"
	expired := time.Now().Add(-time.Minute)
	fragment.LeaseUntil = &expired
	require.NoError(t, db.Create(fragment).Error)

	stored, granted, err := repo.ReserveWikiGenerationFragment(
		ctx, testWikiGenerationFragment(), "new-owner", time.Now().Add(time.Minute), 3,
	)
	require.NoError(t, err)
	require.False(t, granted)
	require.Equal(t, types.WikiGenerationFragmentAmbiguous, stored.State)
	require.Equal(t, 1, stored.Attempts)
}

func TestWikiGenerationFragmentLiveLeaseDoesNotConsumeAnotherAttempt(t *testing.T) {
	repo, _ := setupWikiGenerationFragmentRepo(t)
	ctx := context.Background()
	first, granted, err := repo.ReserveWikiGenerationFragment(
		ctx, testWikiGenerationFragment(), "owner", time.Now().Add(time.Minute), 3,
	)
	require.NoError(t, err)
	require.True(t, granted)
	second, granted, err := repo.ReserveWikiGenerationFragment(
		ctx, testWikiGenerationFragment(), "other", time.Now().Add(time.Minute), 3,
	)
	require.NoError(t, err)
	require.False(t, granted)
	require.Equal(t, types.WikiGenerationFragmentCalling, second.State)
	require.Equal(t, first.CallID, second.CallID)
	require.Equal(t, 1, second.Attempts)
}

func TestWikiGenerationFragmentSuccessSettlementIsScopeBound(t *testing.T) {
	repo, db := setupWikiGenerationFragmentRepo(t)
	ctx := context.Background()
	for _, work := range []string{"work-a", "work-b"} {
		candidate := testWikiGenerationFragment()
		candidate.FragmentID = "fragment-" + work
		candidate.WorkRevision = work
		fragment, granted, err := repo.ReserveWikiGenerationFragment(
			ctx, candidate, "owner-"+work, time.Now().Add(time.Minute), 3,
		)
		require.NoError(t, err)
		require.True(t, granted)
		require.NoError(t, repo.CompleteWikiGenerationFragment(ctx, fragment.FragmentID, fragment.CallID, "output"))
	}
	require.NoError(t, repo.MarkWikiGenerationFragmentsSucceeded(ctx, "work-a"))
	var first, second types.WikiGenerationFragment
	require.NoError(t, db.First(&first, "fragment_id = ?", "fragment-work-a").Error)
	require.NoError(t, db.First(&second, "fragment_id = ?", "fragment-work-b").Error)
	require.Equal(t, types.WikiGenerationFragmentSucceeded, first.State)
	require.Equal(t, types.WikiGenerationFragmentGenerated, second.State)
}
