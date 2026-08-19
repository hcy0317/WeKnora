package handler

import (
	"errors"
	"testing"

	"github.com/Tencent/WeKnora/internal/models/asr"
	"github.com/stretchr/testify/require"
)

func TestClassifyASRTestResultTreatsEveryErrorAsUnavailable(t *testing.T) {
	for _, testError := range []error{
		errors.New("upstream returned 500"),
		errors.New("context deadline exceeded"),
		errors.New("unexpected response payload"),
	} {
		available, message := classifyASRTestResult(nil, testError)
		require.False(t, available)
		require.Contains(t, message, testError.Error())
	}
}

func TestClassifyASRTestResultRequiresAValidResponse(t *testing.T) {
	available, _ := classifyASRTestResult(nil, nil)
	require.False(t, available)

	available, message := classifyASRTestResult(&asr.TranscriptionResult{}, nil)
	require.True(t, available)
	require.Equal(t, "ASR连接成功", message)
}
