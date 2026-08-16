package types

import "time"

// Span kinds — kept narrow because every kind has dedicated rendering on
// the frontend timeline:
//
//   - SpanKindRoot     — the per-(knowledge, attempt) trace root. Always
//     the parent_span_id ancestor of every other span
//     in that attempt. UI uses it for total elapsed.
//   - SpanKindStage    — one of the 5 canonical stages (DocReader, etc.).
//     UI renders these as the timeline segments.
//   - SpanKindSubSpan  — anything inside a stage (e.g. multimodal.image[i]).
//     UI shows them as collapsible children.
//   - SpanKindGeneration — a SubSpan that wraps an LLM/VLM call. Same UI
//     treatment as SubSpan but tagged so we can stitch
//     to the matching Langfuse generation.
const (
	SpanKindRoot       = "root"
	SpanKindStage      = "stage"
	SpanKindSubSpan    = "subspan"
	SpanKindGeneration = "generation"
)

// Span statuses. We deliberately distinguish "failed" (this span itself
// errored) from "cancelled" (an upstream span failed and we abandoned this
// one without running it) so the UI can render the cause differently —
// "you broke X, so we never ran Y" vs. "Y itself broke".
const (
	SpanStatusPending   = "pending"
	SpanStatusRunning   = "running"
	SpanStatusDone      = "done"
	SpanStatusFailed    = "failed"
	SpanStatusSkipped   = "skipped"   // intentionally not run (e.g. multimodal on a text-only doc)
	SpanStatusCancelled = "cancelled" // not run because an upstream span failed
)

// Stage names — the closed set the UI builds its 5-segment timeline from.
// Adding a stage requires a coordinated frontend release. SubSpan names
// are free-form (e.g. "multimodal.image[0]") and don't go through this
// list.
const (
	StageDocReader   = "docreader"
	StageChunking    = "chunking"
	StageEmbedding   = "embedding"
	StageMultimodal  = "multimodal"
	StagePostProcess = "postprocess"
)

// AllStages is the canonical, ordered stage list. Used by the API layer
// to synthesize "pending" placeholders so the timeline always renders five
// segments even before parsing starts.
var AllStages = []string{
	StageDocReader,
	StageChunking,
	StageEmbedding,
	StageMultimodal,
	StagePostProcess,
}

// StageDependencies declares the DAG between stages. Used by the tracker
// to cascade-cancel dependents when a stage fails — a Chunking failure
// silently turns Embedding/Multimodal/PostProcess into "cancelled" so the
// timeline shows a clear blast radius instead of three pending spinners.
//
// Important: Multimodal does NOT depend on Embedding. They share Chunking
// as their upstream and are otherwise independent (Multimodal kicks off
// regardless of vector indexing config). PostProcess joins both before
// running its handlers.
var StageDependencies = map[string][]string{
	StageDocReader:   nil,
	StageChunking:    {StageDocReader},
	StageEmbedding:   {StageChunking},
	StageMultimodal:  {StageChunking},
	StagePostProcess: {StageEmbedding, StageMultimodal},
}

// KnowledgeProcessingSpan is one row in knowledge_processing_spans.
//
// Field tags pull double duty: GORM (storage) and JSON (API). ErrorDetail
// is excluded by default — handlers must opt in for admin views, matching
// how the dead-letter middleware already protects raw stack traces.
type KnowledgeProcessingSpan struct {
	ID           int64                     `gorm:"primaryKey;column:id"             json:"-"`
	KnowledgeID  string                    `gorm:"column:knowledge_id"              json:"knowledge_id"`
	Attempt      int                       `gorm:"column:attempt"                   json:"attempt"`
	SpanID       string                    `gorm:"column:span_id;size:64"           json:"span_id"`
	ParentSpanID string                    `gorm:"column:parent_span_id;size:64"    json:"parent_span_id,omitempty"`
	Name         string                    `gorm:"column:name;size:255"             json:"name"`
	Kind         string                    `gorm:"column:kind;size:16"              json:"kind"`
	Status       string                    `gorm:"column:status;size:16"            json:"status"`
	Input        JSONMap                   `gorm:"column:input;type:jsonb"          json:"input,omitempty"`
	Output       JSONMap                   `gorm:"column:output;type:jsonb"         json:"output,omitempty"`
	Metadata     JSONMap                   `gorm:"column:metadata;type:jsonb"       json:"metadata,omitempty"`
	ErrorCode    string                    `gorm:"column:error_code;size:64"        json:"error_code,omitempty"`
	ErrorMessage string                    `gorm:"column:error_message;type:text"   json:"error_message,omitempty"`
	ErrorDetail  string                    `gorm:"column:error_detail;type:text"    json:"-"`
	StartedAt    *time.Time                `gorm:"column:started_at"                json:"started_at,omitempty"`
	FinishedAt   *time.Time                `gorm:"column:finished_at"               json:"finished_at,omitempty"`
	DurationMs   int64                     `gorm:"column:duration_ms"               json:"duration_ms,omitempty"`
	CreatedAt    time.Time                 `gorm:"column:created_at;autoCreateTime" json:"created_at"`
	UpdatedAt    time.Time                 `gorm:"column:updated_at;autoUpdateTime" json:"updated_at"`
	RetryAction  *KnowledgeSpanRetryAction `gorm:"-" json:"retry_action,omitempty"`
}

// KnowledgeSpanRetryAction is the server-authorized action projected onto a
// failed timeline node. The client never has to duplicate attempt/owner rules.
type KnowledgeSpanRetryAction struct {
	Allowed bool   `json:"allowed"`
	Target  string `json:"target,omitempty"`
	State   string `json:"state,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

const (
	KnowledgeSpanRetryStateFailed  = "failed"
	KnowledgeSpanRetryStateStalled = "stalled"
	KnowledgeSpanRetryStateActive  = "active"
	KnowledgeSpanRetryStateUnknown = "unknown"
)

// KnowledgeProcessingLogicalChildIdentity is the durable identity used to
// reduce retries of the same logical child. Storage and delivery identifiers
// are deliberately excluded: retries keep the same tuple even though each
// execution gets a fresh span_id and database row.
type KnowledgeProcessingLogicalChildIdentity struct {
	KnowledgeID      string
	Attempt          int
	ParentBranchName string
	LogicalChildName string
}

// LogicalChildIdentity returns the stable reducer identity for a span beneath
// the named logical parent branch.
func (s KnowledgeProcessingSpan) LogicalChildIdentity(parentBranchName string) KnowledgeProcessingLogicalChildIdentity {
	return KnowledgeProcessingLogicalChildIdentity{
		KnowledgeID:      s.KnowledgeID,
		Attempt:          s.Attempt,
		ParentBranchName: parentBranchName,
		LogicalChildName: s.Name,
	}
}

// KnowledgeGenerationUsage is the neutral hand-off between the model
// observability layer and the per-document processing trace. Prompts remain in
// Langfuse. Completed model output is mirrored so the in-product timeline does
// not present an unavailable token-usage shell as though it were the response;
// streaming business content is still committed by its caller only after the
// provider's complete event and output validation.
type KnowledgeGenerationUsage struct {
	KnowledgeID      string
	Attempt          int
	TraceID          string
	SpanID           string
	Stage            string
	TaskType         string
	Name             string
	ModelType        string
	ModelID          string
	ModelName        string
	Purpose          string
	Output           JSONMap
	Progress         JSONMap
	InputTokens      int
	OutputTokens     int
	TotalTokens      int
	CacheReadTokens  int
	CacheWriteTokens int
	CacheMissTokens  int
	Unit             string
	Estimated        bool
	UsageAvailable   bool
	Status           string
	ErrorMessage     string
	StartedAt        time.Time
	FinishedAt       time.Time
}

// TableName pins the table because GORM's default pluralization
// ("knowledge_processing_spans") happens to match — explicit beats
// implicit.
func (KnowledgeProcessingSpan) TableName() string {
	return "knowledge_processing_spans"
}

// SpanTreeNode is the API-only tree projection. The repo returns flat
// rows; the handler/tracker assembles SpanTreeNode for the response.
type SpanTreeNode struct {
	KnowledgeProcessingSpan
	Children []*SpanTreeNode `json:"children,omitempty"`
}

// KnowledgeSpanRetryRequest identifies one failed logical owner in the latest
// terminal attempt. ClientRequestID makes retries safe against double-clicks
// and HTTP retries without mutating the failed historical row.
type KnowledgeSpanRetryRequest struct {
	KnowledgeID     string                        `json:"-"`
	Attempt         int                           `json:"-"`
	SpanID          string                        `json:"-"`
	ClientRequestID string                        `json:"client_request_id"`
	Language        string                        `json:"-"`
	StallFence      *KnowledgeSpanRetryStallFence `json:"-"`
}

// KnowledgeSpanMultiRetryTarget identifies one exact logical owner selected
// for an internal aggregate repair. Exact duplicate SpanIDs are idempotently
// collapsed; two different SpanIDs for the same logical owner are rejected by
// the repository as a stale/conflicting selection.
type KnowledgeSpanMultiRetryTarget struct {
	SpanID     string                        `json:"-"`
	StallFence *KnowledgeSpanRetryStallFence `json:"-"`
}

// KnowledgeSpanMultiRetryRequest prepares one fresh partial-repair attempt for
// an exact target set from the same source attempt. This is intentionally an
// internal contract; the public retry endpoint remains single-target.
type KnowledgeSpanMultiRetryRequest struct {
	KnowledgeID     string                          `json:"-"`
	Attempt         int                             `json:"-"`
	ClientRequestID string                          `json:"client_request_id"`
	Language        string                          `json:"-"`
	RequestKind     string                          `json:"-"`
	Targets         []KnowledgeSpanMultiRetryTarget `json:"-"`
	// CarryoverFences proves that unselected pending/running siblings are
	// independently stalled. The repository terminalizes them under the same
	// attempt lock and inherits them as failed evidence without dispatching
	// workers or creating retry outboxes for them.
	CarryoverFences []*KnowledgeSpanRetryStallFence `json:"-"`
}

// KnowledgeSpanAggregateRetryRequest asks the server to select every currently
// authorized failed/stalled owner from one latest attempt. Targets are never
// accepted from HTTP clients.
type KnowledgeSpanAggregateRetryRequest struct {
	KnowledgeID     string `json:"-"`
	Attempt         int    `json:"-"`
	ClientRequestID string `json:"client_request_id"`
	Language        string `json:"-"`
}

type KnowledgeSpanAggregateRetryTarget struct {
	SourceSpanID string `json:"source_span_id"`
	Name         string `json:"target_name"`
	State        string `json:"state"`
	NewSpanID    string `json:"new_span_id,omitempty"`
	TaskID       string `json:"task_id,omitempty"`
}

type KnowledgeSpanAggregateRetryResult struct {
	KnowledgeID     string                              `json:"knowledge_id"`
	SourceAttempt   int                                 `json:"source_attempt"`
	ClientRequestID string                              `json:"client_request_id"`
	Attempt         int                                 `json:"new_attempt"`
	Targets         []KnowledgeSpanAggregateRetryTarget `json:"targets"`
}

type KnowledgeSpanAggregateRetryAction struct {
	Allowed bool                                `json:"allowed"`
	Reason  string                              `json:"reason,omitempty"`
	Targets []KnowledgeSpanAggregateRetryTarget `json:"targets,omitempty"`
	Counts  KnowledgeSpanAggregateRetryCounts   `json:"counts"`
}

type KnowledgeSpanAggregateRetryCounts struct {
	Summary  int `json:"summary"`
	Wiki     int `json:"wiki"`
	Graph    int `json:"graph"`
	Question int `json:"question"`
}

// KnowledgeSpanRetryStallFence is server-issued evidence that a non-terminal
// owner had no live executor. The repository rechecks every DB-owned field
// while holding its transaction locks; HTTP clients can never supply it.
type KnowledgeSpanRetryStallFence struct {
	KnowledgeID       string
	TenantID          uint64
	OwnerName         string
	SourceAttempt     int
	SourceSpanID      string
	SourceUpdatedAt   time.Time
	LatestRootAttempt int
	LastHeartbeatAt   time.Time
	TaskID            string
	Queue             string
	PendingOpIDs      []int64
	ClaimToken        string
	ClaimedByTaskID   string
	ClaimHeartbeatAt  time.Time
}

// KnowledgeSpanRetryTargetSnapshot is the DB half of retry authorization.
// Runtime task, durable claim and recovery-lease evidence are evaluated in the
// service layer before a StallFence can be issued.
type KnowledgeSpanRetryTargetSnapshot struct {
	Source            KnowledgeProcessingSpan
	Parent            KnowledgeProcessingSpan
	LatestRoot        KnowledgeProcessingSpan
	LatestOwnerSpanID string
	TenantID          uint64
	KnowledgeBaseID   string
	KnowledgeStatus   string
	ExistingRetry     bool
}

// KnowledgeSpanRetryPreparation is committed before its worker is published.
// Internal routing fields are excluded from JSON; callers receive only the new
// partial-repair attempt and pending owner identity.
type KnowledgeSpanRetryPreparation struct {
	KnowledgeID      string  `json:"knowledge_id"`
	SourceAttempt    int     `json:"source_attempt"`
	SourceSpanID     string  `json:"source_span_id"`
	ClientRequestID  string  `json:"client_request_id"`
	Attempt          int     `json:"new_attempt"`
	SpanID           string  `json:"new_span_id"`
	Name             string  `json:"target_name"`
	TaskID           string  `json:"task_id"`
	Status           string  `json:"-"`
	ErrorCode        string  `json:"-"`
	ErrorMessage     string  `json:"-"`
	DispatchRequired bool    `json:"-"`
	TenantID         uint64  `json:"-"`
	KnowledgeBaseID  string  `json:"-"`
	Language         string  `json:"-"`
	Input            JSONMap `json:"-"`
}

const (
	KnowledgeSpanRetryOutboxTaskType = "knowledge:span_retry_dispatch"
	KnowledgeSpanRetryOutboxScope    = "knowledge"
	KnowledgeSpanRetryOutboxOp       = "dispatch"
)

// KnowledgeSpanRetryOutboxPayload is the durable dispatch record committed in
// the same transaction as a partial-repair attempt. It contains identifiers,
// never document/model output, and can reconstruct the exact deterministic
// Asynq task after a process exits between commit and publish.
type KnowledgeSpanRetryOutboxPayload struct {
	TaskID          string  `json:"task_id"`
	KnowledgeID     string  `json:"knowledge_id"`
	Attempt         int     `json:"attempt"`
	SpanID          string  `json:"span_id"`
	TargetName      string  `json:"target_name"`
	TenantID        uint64  `json:"tenant_id"`
	KnowledgeBaseID string  `json:"knowledge_base_id"`
	Language        string  `json:"language,omitempty"`
	Input           JSONMap `json:"input,omitempty"`
}
