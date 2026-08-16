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
