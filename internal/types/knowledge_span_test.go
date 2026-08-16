package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLogicalChildIdentityIgnoresDeliveryAndStorageIDs(t *testing.T) {
	first := KnowledgeProcessingSpan{
		ID: 1, KnowledgeID: "kid", Attempt: 7, SpanID: "random-first",
		Name: "postprocess.question.batch[3]",
	}
	retry := first
	retry.ID = 99
	retry.SpanID = "random-retry"

	assert.Equal(t,
		first.LogicalChildIdentity("postprocess.question"),
		retry.LogicalChildIdentity("postprocess.question"),
	)
	assert.Equal(t, KnowledgeProcessingLogicalChildIdentity{
		KnowledgeID: "kid", Attempt: 7,
		ParentBranchName: "postprocess.question",
		LogicalChildName: "postprocess.question.batch[3]",
	}, first.LogicalChildIdentity("postprocess.question"))
}
