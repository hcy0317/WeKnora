package types

import (
	"encoding/json"
	"time"
)

// TaskPendingOp is one entry in the generic task pending queue
// (`task_pending_ops`). The queue is the durable replacement for the
// Redis-list-backed `wiki:pending:<kbID>` queue: rows survive restarts
// and are not subject to TTL eviction.
//
// The (TaskType, Scope, ScopeID) tuple is the queue identity. A consumer
// pulls a batch with PeekBatch on that tuple, deduplicates by DedupKey
// service-side if it cares, processes the ops, then DeleteByIDs the
// consumed rows. FailCount is incremented per-row by IncrFailCount and
// the consumer dead-letters the row once the count exceeds a service-
// defined cap.
type TaskPendingOp struct {
	// Auto-increment row id. Used by PeekBatch ordering and by
	// DeleteByIDs / IncrFailCount as the row key.
	ID int64 `json:"id" gorm:"primaryKey;autoIncrement"`
	// Tenant scope mirrored from the enclosing object so per-tenant
	// retention / quota queries don't have to join.
	TenantID uint64 `json:"tenant_id" gorm:"index"`
	// Free-form task identifier (e.g. "wiki:ingest"). Should match an
	// asynq task type when applicable, but the queue itself doesn't
	// enforce that — it's just a string.
	TaskType string `json:"task_type" gorm:"type:varchar(64)"`
	// Logical scope, e.g. "knowledge_base" / "knowledge" / "tenant".
	// Read together with ScopeID.
	Scope string `json:"scope" gorm:"type:varchar(32)"`
	// Identifier within the scope (e.g. kbID for scope="knowledge_base").
	ScopeID string `json:"scope_id" gorm:"type:varchar(64)"`
	// Operation kind. Service-defined: e.g. "ingest" / "retract" for
	// wiki, but other consumers can use whatever vocabulary they like.
	Op string `json:"op" gorm:"type:varchar(32)"`
	// Optional service-defined deduplication key. The consumer is
	// responsible for collapsing equivalent ops within a peeked batch
	// (the queue itself does NOT enforce uniqueness — multiple rows with
	// the same DedupKey can coexist; the consumer chooses which wins).
	DedupKey string `json:"dedup_key" gorm:"type:varchar(128);default:''"`
	// JSON-serialized op payload. Schema is consumer-defined; the queue
	// stores it verbatim. Use json.RawMessage to avoid double-decode.
	Payload json.RawMessage `json:"payload" gorm:"type:jsonb;default:'{}'"`
	// In-batch retry counter. Reset to 0 on successful consume.
	FailCount int `json:"fail_count" gorm:"default:0"`
	// Server-side enqueue time. NOT used for ordering (id is the cursor)
	// but useful for ops queries like "rows older than 1h that never
	// drained".
	EnqueuedAt time.Time `json:"enqueued_at"`
	// Claim start timestamp. It remains stable for an ownership term;
	// renewable liveness is recorded separately in ClaimHeartbeatAt.
	ClaimedAt *time.Time `json:"claimed_at,omitempty"`
	// ClaimToken is the stable fencing token for one claim ownership term.
	// It never changes during renewal; a successor claim always receives a
	// different token so delayed workers cannot release or acknowledge it.
	ClaimToken string `json:"claim_token,omitempty" gorm:"type:varchar(64)"`
	// ClaimedByTaskID binds the durable claim to the concrete Asynq delivery.
	ClaimedByTaskID string `json:"claimed_by_task_id,omitempty" gorm:"type:varchar(255)"`
	// ClaimHeartbeatAt advances while the owner is alive. Stale recovery uses
	// this timestamp rather than mutating ClaimToken on every renewal.
	ClaimHeartbeatAt *time.Time `json:"claim_heartbeat_at,omitempty"`
}

// TaskClaimOwner is the immutable identity of one pending-op claim term.
type TaskClaimOwner struct {
	Token  string `json:"token"`
	TaskID string `json:"task_id"`
}

func (o TaskClaimOwner) Valid() bool {
	return o.Token != "" && o.TaskID != ""
}

// TaskPendingOpClaimSnapshot is the fail-closed owner evidence for one
// logical dedup key. Consistent is false when rows disagree on ownership.
type TaskPendingOpClaimSnapshot struct {
	Found           bool       `json:"found"`
	Consistent      bool       `json:"consistent"`
	RowIDs          []int64    `json:"row_ids"`
	ClaimToken      string     `json:"claim_token,omitempty"`
	ClaimedByTaskID string     `json:"claimed_by_task_id,omitempty"`
	ClaimedAt       *time.Time `json:"claimed_at,omitempty"`
	HeartbeatAt     *time.Time `json:"heartbeat_at,omitempty"`
}

// ProcessingOwnerRef is the stable logical worker identity used by the
// short-lived processing lease consulted by stalled-owner evaluation.
type ProcessingOwnerRef struct {
	TenantID    uint64 `json:"tenant_id"`
	KnowledgeID string `json:"knowledge_id"`
	Attempt     int    `json:"attempt"`
	Name        string `json:"name"`
}

func (r ProcessingOwnerRef) Valid() bool {
	return r.KnowledgeID != "" && r.Attempt > 0 && r.Name != ""
}

type ProcessingOwnerLeaseSnapshot struct {
	Active bool           `json:"active"`
	Owner  TaskClaimOwner `json:"owner"`
	TTL    time.Duration  `json:"ttl"`
}

// TableName binds TaskPendingOp to the `task_pending_ops` table.
func (TaskPendingOp) TableName() string {
	return "task_pending_ops"
}
