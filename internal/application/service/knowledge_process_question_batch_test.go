package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuestionBatchAllFailedError_ReturnsFirstFailureWhenEveryCallFails(t *testing.T) {
	firstErr := errors.New("upstream unavailable")

	err := questionBatchAllFailedError(3, 3, firstErr)

	require.Error(t, err)
	assert.ErrorIs(t, err, firstErr)
	assert.Contains(t, err.Error(), "all 3 question generation calls failed")
}

func TestQuestionBatchAllFailedError_ToleratesPartialFailure(t *testing.T) {
	firstErr := errors.New("one chunk failed")

	err := questionBatchAllFailedError(3, 1, firstErr)

	assert.NoError(t, err)
}

func TestQuestionBatchShouldSettleParent_OnlyOnTerminalDelivery(t *testing.T) {
	retryErr := errors.New("retry me")

	assert.True(t, questionBatchShouldSettleParent(nil, false))
	assert.False(t, questionBatchShouldSettleParent(retryErr, false))
	assert.True(t, questionBatchShouldSettleParent(retryErr, true))
}
