package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/Tencent/WeKnora/internal/logger"
)

// inspectByQueryResponse parses an _update_by_query / _delete_by_query
// response and surfaces partial failures without leaking cluster-side reason
// strings (which may embed document content). The returned error carries only
// bounded IDs/types, and every timeout, conflict, or failure is fail-closed.
func inspectByQueryResponse(body io.Reader) error {
	var r struct {
		TimedOut         *bool `json:"timed_out"`
		VersionConflicts *int  `json:"version_conflicts"`
		Failures         *[]struct {
			ID    string `json:"id"`
			Cause struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"cause"`
		} `json:"failures"`
	}
	if err := json.NewDecoder(body).Decode(&r); err != nil {
		return fmt.Errorf("opensearch: parse by-query response: %w", ErrTransport)
	}
	if r.TimedOut == nil || r.VersionConflicts == nil || r.Failures == nil {
		return fmt.Errorf("opensearch: incomplete by-query response: %w", ErrTransport)
	}
	if *r.TimedOut {
		return fmt.Errorf("opensearch: by-query timed out: %w", ErrTransport)
	}
	if *r.VersionConflicts > 0 {
		return fmt.Errorf("opensearch: by-query had %d version conflicts: %w", *r.VersionConflicts, ErrTransport)
	}
	log := logger.GetLogger(context.Background())
	if len(*r.Failures) == 0 {
		return nil
	}
	var msgs []string
	for _, f := range *r.Failures {
		// Full reason → debug log only (may contain document content).
		log.Debugf("[OpenSearch] by-query failure: id=%s type=%s", f.ID, f.Cause.Type)
		if len(msgs) < 5 {
			msgs = append(msgs, fmt.Sprintf("[%s] %s", f.ID, f.Cause.Type))
		}
	}
	return fmt.Errorf("opensearch: by-query partial failure (%d failed, first 5: %s): %w",
		len(*r.Failures), strings.Join(msgs, "; "), ErrTransport)
}
