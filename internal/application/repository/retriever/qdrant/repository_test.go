package qdrant

import (
	"strings"
	"testing"

	qdrantapi "github.com/qdrant/go-client/qdrant"
)

func TestValidateUpdateResultRequiresCompleted(t *testing.T) {
	tests := []struct {
		name   string
		result *qdrantapi.UpdateResult
		wantOK bool
	}{
		{name: "nil", result: nil},
		{name: "unknown", result: &qdrantapi.UpdateResult{Status: qdrantapi.UpdateStatus_UnknownUpdateStatus}},
		{name: "acknowledged", result: &qdrantapi.UpdateResult{Status: qdrantapi.UpdateStatus_Acknowledged}},
		{name: "clock rejected", result: &qdrantapi.UpdateResult{Status: qdrantapi.UpdateStatus_ClockRejected}},
		{name: "wait timeout", result: &qdrantapi.UpdateResult{Status: qdrantapi.UpdateStatus_WaitTimeout}},
		{name: "completed", result: &qdrantapi.UpdateResult{Status: qdrantapi.UpdateStatus_Completed}, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateUpdateResult(tt.result)
			if tt.wantOK && err != nil {
				t.Fatalf("completed result rejected: %v", err)
			}
			if !tt.wantOK && err == nil {
				t.Fatal("non-completed result accepted")
			}
		})
	}
}

func TestWaitForQdrantCompletionIsEnabled(t *testing.T) {
	wait := waitForQdrantCompletion()
	if wait == nil || !*wait {
		t.Fatal("write request must wait for completion")
	}
}

func TestValidateUpdateResultErrorDoesNotExposeOperationID(t *testing.T) {
	operationID := uint64(424242)
	err := validateUpdateResult(&qdrantapi.UpdateResult{
		OperationId: &operationID,
		Status:      qdrantapi.UpdateStatus_WaitTimeout,
	})
	if err == nil {
		t.Fatal("wait timeout must fail")
	}
	if strings.Contains(err.Error(), "424242") {
		t.Fatalf("error leaked backend operation details: %v", err)
	}
}
