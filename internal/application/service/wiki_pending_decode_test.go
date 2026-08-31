package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func TestDecodePendingRowsDoesNotCollapseLegacyRowsByKnowledgeAndAttempt(t *testing.T) {
	rows := []*types.TaskPendingOp{
		{ID: 11, Op: WikiOpIngest, DedupKey: "kid", Payload: json.RawMessage(`{"op":"ingest","knowledge_id":"kid","attempt":3}`)},
		{ID: 12, Op: WikiOpIngest, DedupKey: "kid", Payload: json.RawMessage(`{"op":"ingest","knowledge_id":"kid","attempt":3}`)},
	}
	ops, ids := (&wikiIngestService{}).decodePendingRows(context.Background(), rows)
	require.Equal(t, []int64{11, 12}, ids)
	require.Len(t, ops, 2, "attempt alone is not a revision/work identity")
	require.Equal(t, []int64{11}, ops[0].pendingRowIDs())
	require.Equal(t, []int64{12}, ops[1].pendingRowIDs())
}

func TestPartitionWikiUnappliedOpsKeepsDistinctWorkIDsOutOfTrimSet(t *testing.T) {
	workA := WikiPendingOp{KnowledgeID: "kid", WorkID: "work-a", dbID: 11, dbIDs: []int64{11}}
	workB := WikiPendingOp{KnowledgeID: "kid", WorkID: "work-b", dbID: 12, dbIDs: []int64{12}}
	terminal := WikiPendingOp{KnowledgeID: "other", WorkID: "work-c", dbID: 13, dbIDs: []int64{13}}
	unapplied := map[wikiWorkKey]struct{}{
		wikiPendingOpWorkKey(workA): {},
		wikiPendingOpWorkKey(workB): {},
	}

	failed, deferred := partitionWikiUnappliedOps(
		[]WikiPendingOp{workA, workB, terminal}, nil, unapplied, nil,
	)

	require.Empty(t, failed)
	require.Equal(t, []WikiPendingOp{workA, workB}, deferred)
	require.Equal(t, []int64{13}, wikiPendingTrimIDs([]int64{11, 12, 13}, failed, deferred))
}

func TestBindMappedWikiPendingOpsCarriesPreparedWorkIdentityIntoSettlement(t *testing.T) {
	initial := WikiPendingOp{KnowledgeID: "kid", dbID: 21, dbIDs: []int64{21}}
	other := WikiPendingOp{KnowledgeID: "other", dbID: 22, dbIDs: []int64{22}}
	bound := bindMappedWikiPendingOps([]WikiPendingOp{initial, other}, []*docIngestResult{{
		KnowledgeID: "kid", WorkID: "work-prepared", SourceOp: initial,
	}})

	require.Equal(t, "work-prepared", bound[0].WorkID)
	workKey := wikiSlugUpdateWorkKey(SlugUpdate{KnowledgeID: "kid", WorkID: "work-prepared"})

	t.Run("contention defers the mapped row", func(t *testing.T) {
		failed, deferred := partitionWikiUnappliedOps(
			bound, nil, map[wikiWorkKey]struct{}{workKey: {}}, nil,
		)
		require.Empty(t, failed)
		require.Equal(t, []WikiPendingOp{bound[0]}, deferred)
		require.Equal(t, []int64{22}, wikiPendingTrimIDs([]int64{21, 22}, failed, deferred))
	})

	t.Run("real failure charges and retains the mapped row", func(t *testing.T) {
		failed, deferred := partitionWikiUnappliedOps(
			bound, nil,
			map[wikiWorkKey]struct{}{workKey: {}},
			map[wikiWorkKey]struct{}{workKey: {}},
		)
		require.Equal(t, []WikiPendingOp{bound[0]}, failed)
		require.Empty(t, deferred)
		require.Equal(t, []int64{22}, wikiPendingTrimIDs([]int64{21, 22}, failed, deferred))
	})
}
