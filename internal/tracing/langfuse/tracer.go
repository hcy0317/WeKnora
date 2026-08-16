package langfuse

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/Tencent/WeKnora/internal/logger"
	"github.com/Tencent/WeKnora/internal/types"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Trace represents an active root observation. A Trace is conceptually one
// "request" (e.g. a chat turn). Generations and spans attached to it roll up
// as children in the Langfuse UI. It wraps an OpenTelemetry root span; its
// ID is the OTel trace id (W3C 32-hex), which — when the request carried a
// traceparent header — is the upstream caller's trace id (sop3 correlation).
type Trace struct {
	ID      string
	span    trace.Span
	manager *Manager
	mu      sync.Mutex
	// metadata holds the metadata set at StartTrace so Finish can merge (not
	// overwrite) the finish-time metadata into it before serializing.
	metadata map[string]interface{}
}

// Generation represents a single model invocation (LLM / embedding / VLM / ASR).
type Generation struct {
	ID         string
	span       trace.Span
	manager    *Manager
	model      string
	name       string
	ctx        context.Context
	metadata   map[string]interface{}
	progressMu sync.Mutex
	progress   types.JSONMap
	knowledge  KnowledgeTraceContext
	startedAt  time.Time
	// autoTrace is a non-nil root trace this generation implicitly opened
	// because ctx carried none; Finish must End it so the root is exported.
	autoTrace *Trace
}

// Span represents a logical unit of work that isn't itself an LLM call — for
// example an asynq task execution, a pipeline stage, or a document-processing
// step. Generations and nested spans attach as children via the OTel span
// context (parenting is automatic through trace.SpanFromContext).
type Span struct {
	ID      string
	span    trace.Span
	manager *Manager
	name    string
	// metadata holds the metadata set at StartSpan so Finish can merge (not
	// overwrite) the finish-time metadata into it before serializing.
	metadata map[string]interface{}
	// autoTrace is a non-nil root trace this span implicitly opened because
	// ctx carried none; Finish must End it so the root is exported.
	autoTrace *Trace
}

// TraceOptions configures a new trace.
type TraceOptions struct {
	Name        string
	UserID      string
	SessionID   string
	Input       interface{}
	Metadata    map[string]interface{}
	Tags        []string
	Environment string
	Release     string
}

// GenerationOptions configures a new generation observation.
type GenerationOptions struct {
	Name            string
	Model           string
	Input           interface{}
	Metadata        map[string]interface{}
	ModelParameters map[string]interface{}
}

// SpanOptions configures a new SPAN observation.
type SpanOptions struct {
	Name     string
	Input    interface{}
	Metadata map[string]interface{}
}

// StartTrace opens a root span. When ctx carries a remote SpanContext (from a
// W3C traceparent extracted by GinMiddleware), the root span inherits the
// upstream trace id — this is what makes a sop3 run and its WeKnora call land
// under the same trace in LiteFuse. The returned *Trace is non-nil even when
// disabled (methods are no-ops), so callers don't need nil checks.
func (m *Manager) StartTrace(ctx context.Context, opts TraceOptions) (context.Context, *Trace) {
	if !m.Enabled() {
		return ctx, &Trace{manager: m}
	}
	name := opts.Name
	attrs := []attribute.KeyValue{attribute.String(attrObsType, obsTypeTrace)}
	if opts.Name != "" {
		attrs = append(attrs, attribute.String(attrTraceName, opts.Name))
	}
	if opts.UserID != "" {
		attrs = append(attrs, attribute.String(attrUserID, opts.UserID))
	}
	if opts.SessionID != "" {
		attrs = append(attrs, attribute.String(attrSessionID, opts.SessionID))
	}
	env := opts.Environment
	if env == "" {
		env = m.cfg.Environment
	}
	if env != "" {
		attrs = append(attrs, attribute.String(attrEnvironment, env))
	}
	rel := opts.Release
	if rel == "" {
		rel = m.cfg.Release
	}
	if rel != "" {
		attrs = append(attrs, attribute.String(attrRelease, rel))
	}
	attrs = append(attrs, jsonAttr(attrTraceInput, opts.Input))
	attrs = append(attrs, jsonAttr(attrTraceMetadata, opts.Metadata))
	if len(opts.Tags) > 0 {
		attrs = append(attrs, jsonAttr(attrTraceTags, opts.Tags))
	}
	ctx, span := m.tracer.Start(ctx, name, trace.WithTimestamp(time.Now()), trace.WithAttributes(attrs...))
	t := &Trace{ID: span.SpanContext().TraceID().String(), span: span, manager: m, metadata: opts.Metadata}
	return withTrace(ctx, t), t
}

// Finish updates the trace with its final output and merges any finish-time
// metadata into the metadata set at StartTrace. Safe to call on a disabled
// trace (no-op). Finish keys are merged on top of the open-time correlation
// fields (request_id, http.method, etc.) rather than overwriting them, so
// both the open's correlation and the finish outcome survive.
func (t *Trace) Finish(output interface{}, metadata map[string]interface{}) {
	if t == nil || t.manager == nil || !t.manager.Enabled() || t.span == nil {
		return
	}
	attrs := []attribute.KeyValue{jsonAttr(attrTraceOutput, output)}
	t.mu.Lock()
	t.metadata = mergeMetadata(t.metadata, metadata)
	merged := mergeMetadata(t.metadata, nil)
	t.mu.Unlock()
	if merged != nil {
		attrs = append(attrs, jsonAttr(attrTraceMetadata, merged))
	}
	t.span.SetAttributes(attrs...)
	t.span.End()
}

// annotateKnowledgeID adds document correlation at trace level while the
// originating root span is still local. A single-document request keeps a
// scalar value for convenient filtering; a batch request promotes the field
// to a de-duplicated array instead of silently overwriting an earlier ID.
func (t *Trace) annotateKnowledgeID(knowledgeID string) {
	if t == nil || t.span == nil || knowledgeID == "" {
		return
	}
	t.mu.Lock()
	if t.metadata == nil {
		t.metadata = make(map[string]interface{})
	}
	t.metadata["knowledge_id"] = mergeKnowledgeID(t.metadata["knowledge_id"], knowledgeID)
	metadata := mergeMetadata(t.metadata, nil)
	t.mu.Unlock()
	t.span.SetAttributes(jsonAttr(attrTraceMetadata, metadata))
}

func mergeKnowledgeID(current interface{}, knowledgeID string) interface{} {
	switch value := current.(type) {
	case nil:
		return knowledgeID
	case string:
		if value == knowledgeID {
			return value
		}
		return []string{value, knowledgeID}
	case []string:
		for _, existing := range value {
			if existing == knowledgeID {
				return value
			}
		}
		return append(value, knowledgeID)
	case []interface{}:
		for _, existing := range value {
			if existing == knowledgeID {
				return value
			}
		}
		return append(value, knowledgeID)
	default:
		return knowledgeID
	}
}

// ResumeTrace reconstructs a *Trace handle from an externally-provided W3C
// trace id (and optional parent span id), without creating a new root span —
// the originating process (e.g. an HTTP request that already opened a trace)
// owns the root. Used to graft async work onto an existing trace: it sets a
// remote SpanContext on ctx so any child span/generation started under it
// inherits the upstream trace id. When traceID is empty the returned *Trace
// is nil, signalling the caller should fall back to StartTrace.
func (m *Manager) ResumeTrace(ctx context.Context, traceID, parentSpanID string) (context.Context, *Trace) {
	if m == nil || !m.Enabled() || traceID == "" {
		return ctx, nil
	}
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		// Not a W3C 32-hex trace id (legacy UUID, etc.); cannot resume.
		return ctx, nil
	}
	var sid trace.SpanID
	if parentSpanID != "" {
		if s, err := trace.SpanIDFromHex(parentSpanID); err == nil {
			sid = s
		}
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	t := &Trace{ID: traceID, manager: m}
	return withTrace(ctx, t), t
}

// reestablishParentSpan re-injects the active trace's root span as the OTel
// parent when ctx carries a *Trace but no active OTel span. This happens when
// a context rebuild drops the OTel span while the *Trace handle survives on
// the exported key (e.g. a background goroutine derived from a non-request
// context, or a CloneContext that predates/missed the span fix). Without
// this, child spans (e.g. a summary generation) start a fresh root and orphan
// off the HTTP trace.
func (m *Manager) reestablishParentSpan(ctx context.Context) context.Context {
	if !m.Enabled() {
		return ctx
	}
	if sp := trace.SpanFromContext(ctx); sp.IsRecording() {
		return ctx // already has an active span
	}
	if t, ok := traceFromCtx(ctx); ok && t != nil && t.span != nil {
		return trace.ContextWithSpan(ctx, t.span)
	}
	return ctx
}

// StartSpan opens a child span under the trace/span carried by ctx. When no
// trace is present, OTel creates a fresh root (mirroring StartGeneration's
// auto-trace behaviour). Returns a ctx whose active span is this span.
func (m *Manager) StartSpan(ctx context.Context, opts SpanOptions) (context.Context, *Span) {
	if !m.Enabled() {
		return ctx, &Span{manager: m}
	}
	ctx = m.reestablishParentSpan(ctx)
	var autoTrace *Trace
	if _, ok := traceFromCtx(ctx); !ok {
		// No active trace: open a shallow root so the span isn't orphaned.
		// Hold the handle so Finish can End it — otherwise the root span is
		// never exported and this span's parent points at a missing span.
		ctx, autoTrace = m.StartTrace(ctx, TraceOptions{Name: opts.Name})
	}
	attrs := []attribute.KeyValue{
		attribute.String(attrObsType, obsTypeSpan),
		jsonAttr(attrObsInput, opts.Input),
		jsonAttr(attrObsMetadata, opts.Metadata),
	}
	ctx, span := m.tracer.Start(ctx, opts.Name, trace.WithTimestamp(time.Now()), trace.WithAttributes(attrs...))
	return ctx, &Span{
		ID:        span.SpanContext().SpanID().String(),
		span:      span,
		manager:   m,
		name:      opts.Name,
		metadata:  opts.Metadata,
		autoTrace: autoTrace,
	}
}

// Finish updates a span with its final output, extra metadata and any error.
// A non-nil err marks the span as ERROR. Finish-time metadata is merged on top
// of the metadata set at StartSpan (finish keys win) rather than discarded, so
// fields only known at completion (outcome, duration_ms, tool_calls, …) are
// reported. If this span implicitly opened a root trace, that root is ended
// last so it is exported.
func (s *Span) Finish(output interface{}, metadata map[string]interface{}, err error) {
	if s == nil || s.manager == nil || !s.manager.Enabled() || s.span == nil {
		return
	}
	attrs := []attribute.KeyValue{jsonAttr(attrObsOutput, output)}
	if merged := mergeMetadata(s.metadata, metadata); merged != nil {
		attrs = append(attrs, jsonAttr(attrObsMetadata, merged))
	}
	s.span.SetAttributes(attrs...)
	if err != nil {
		s.span.RecordError(err)
		s.span.SetStatus(codes.Error, err.Error())
	}
	s.span.End()
	if s.autoTrace != nil {
		s.autoTrace.Finish(nil, nil)
	}
}

// StartGeneration opens a generation observation under the trace carried by
// ctx (or a newly auto-created trace). If a parent span is present on ctx,
// the generation attaches under it via the OTel span context.
func (m *Manager) StartGeneration(ctx context.Context, opts GenerationOptions) (context.Context, *Generation) {
	if !m.Enabled() {
		return ctx, &Generation{manager: m, model: opts.Model, name: opts.Name}
	}
	ctx = m.reestablishParentSpan(ctx)
	var autoTrace *Trace
	if _, ok := traceFromCtx(ctx); !ok {
		// No active trace: open a root so the generation isn't orphaned, and
		// hold the handle so Finish can End it (otherwise the root span never
		// gets exported and this generation's parent points at nothing).
		ctx, autoTrace = m.StartTrace(ctx, TraceOptions{Name: opts.Name})
	}
	startedAt := time.Now()
	knowledge, _ := knowledgeTraceContextFromContext(ctx)
	purpose, _ := opts.Metadata["call_purpose"].(string)
	metadata := enrichGenerationMetadata(opts.Metadata, knowledge, opts.Name, purpose)
	attrs := []attribute.KeyValue{
		attribute.String(attrObsType, obsTypeGeneration),
		attribute.String(attrObsModel, opts.Model),
		jsonAttr(attrObsInput, opts.Input),
		jsonAttr(attrObsMetadata, metadata),
		jsonAttr(attrObsModelParams, opts.ModelParameters),
	}
	ctx, span := m.tracer.Start(ctx, opts.Name, trace.WithTimestamp(startedAt), trace.WithAttributes(attrs...))
	g := &Generation{
		ID:       span.SpanContext().SpanID().String(),
		span:     span,
		manager:  m,
		model:    opts.Model,
		name:     opts.Name,
		ctx:      ctx,
		metadata: metadata,
		progress: types.JSONMap{
			"state":      "created",
			"created_at": startedAt.UTC().Format(time.RFC3339Nano),
		},
		knowledge: knowledge,
		startedAt: startedAt,
		autoTrace: autoTrace,
	}
	// Mirror the open observation immediately. Long-running Wiki Responses
	// streams can spend many minutes before the first answer token; waiting for
	// Finish made the processing timeline look as though no LLM request existed.
	// Finish upserts the same span id, while retries use fresh ids and therefore
	// remain visible as separate historical attempts.
	g.recordKnowledgeUsage(nil, nil, nil, time.Time{})
	return ctx, g
}

// RecordProgress mirrors a coarse transport milestone into the same running
// generation row. The model body is intentionally absent; only the final
// completed output is persisted by Finish.
func (g *Generation) RecordProgress(progress GenerationProgress) {
	if g == nil || g.manager == nil || !g.manager.Enabled() || g.span == nil || progress.State == "" {
		return
	}
	if progress.At.IsZero() {
		progress.At = time.Now()
	}
	g.progressMu.Lock()
	if g.progress == nil {
		g.progress = types.JSONMap{}
	}
	g.progress["state"] = progress.State
	g.progress[progress.State+"_at"] = progress.At.UTC().Format(time.RFC3339Nano)
	if progress.Protocol != "" {
		g.progress["protocol"] = progress.Protocol
	}
	if progress.Endpoint != "" {
		g.progress["endpoint"] = progress.Endpoint
	}
	if progress.EventType != "" {
		g.progress["first_event_type"] = progress.EventType
	}
	g.progressMu.Unlock()
	g.recordKnowledgeUsage(nil, nil, nil, time.Time{})
}

// Finish updates a generation with its final output, token usage and any
// error. A non-nil err marks the observation as ERROR.
func (g *Generation) Finish(output interface{}, usage *TokenUsage, err error) {
	if g == nil || g.manager == nil || !g.manager.Enabled() || g.span == nil {
		return
	}
	finishedAt := time.Now()
	attrs := []attribute.KeyValue{jsonAttr(attrObsOutput, output)}
	if usage != nil {
		attrs = append(attrs, jsonAttr(attrObsUsageDetails, usage))
	}
	g.span.SetAttributes(attrs...)
	if err != nil {
		g.span.RecordError(err)
		g.span.SetStatus(codes.Error, err.Error())
	}
	g.span.End()
	g.recordKnowledgeUsage(output, usage, err, finishedAt)
	if g.autoTrace != nil {
		g.autoTrace.Finish(nil, nil)
	}
}

func (g *Generation) recordKnowledgeUsage(
	output interface{},
	usage *TokenUsage,
	callErr error,
	finishedAt time.Time,
) {
	if g == nil || g.manager == nil || g.knowledge.KnowledgeID == "" || g.knowledge.Attempt <= 0 {
		return
	}
	modelType := modelTypeFromGenerationName(g.name)
	purpose, _ := g.metadata["call_purpose"].(string)
	modelID, _ := g.metadata["model_id"].(string)
	status := types.SpanStatusRunning
	if !finishedAt.IsZero() {
		status = types.SpanStatusDone
	}
	record := types.KnowledgeGenerationUsage{
		KnowledgeID:    g.knowledge.KnowledgeID,
		Attempt:        g.knowledge.Attempt,
		TraceID:        g.span.SpanContext().TraceID().String(),
		SpanID:         g.ID,
		Stage:          resolveGenerationStage(g.knowledge, g.name, purpose),
		TaskType:       g.knowledge.TaskType,
		Name:           g.name,
		ModelType:      modelType,
		ModelID:        modelID,
		ModelName:      g.model,
		Purpose:        purpose,
		Output:         normalizeKnowledgeGenerationOutput(output),
		Progress:       g.snapshotProgress(),
		Estimated:      generationUsageEstimated(modelType) && usage != nil,
		UsageAvailable: usage != nil,
		Unit:           "TOKENS",
		Status:         status,
		StartedAt:      g.startedAt,
		FinishedAt:     finishedAt,
	}
	if usage != nil {
		record.InputTokens = usage.Input
		record.OutputTokens = usage.Output
		record.TotalTokens = usage.Total
		if record.TotalTokens == 0 {
			record.TotalTokens = usage.Input + usage.Output
		}
		record.CacheReadTokens = usage.CacheRead
		record.CacheWriteTokens = usage.CacheWrite
		record.CacheMissTokens = usage.CacheMiss
		if usage.Unit != "" {
			record.Unit = usage.Unit
		}
	}
	if callErr != nil {
		record.Status = types.SpanStatusFailed
		if errors.Is(callErr, context.Canceled) {
			record.Status = types.SpanStatusCancelled
		}
		record.ErrorMessage = callErr.Error()
	}
	g.manager.recordKnowledgeUsage(context.WithoutCancel(g.ctx), record)
}

func (g *Generation) snapshotProgress() types.JSONMap {
	if g == nil {
		return nil
	}
	g.progressMu.Lock()
	defer g.progressMu.Unlock()
	if len(g.progress) == 0 {
		return nil
	}
	copy := make(types.JSONMap, len(g.progress))
	for key, value := range g.progress {
		copy[key] = value
	}
	return copy
}

func normalizeKnowledgeGenerationOutput(output interface{}) types.JSONMap {
	switch value := output.(type) {
	case nil:
		return nil
	case types.JSONMap:
		copy := make(types.JSONMap, len(value))
		for key, item := range value {
			copy[key] = item
		}
		return copy
	case map[string]interface{}:
		copy := make(types.JSONMap, len(value))
		for key, item := range value {
			copy[key] = item
		}
		return copy
	default:
		return types.JSONMap{"content": value}
	}
}

// MarkCompletionStart records the time at which the first token was received
// in a streaming generation. Langfuse surfaces this as time-to-first-token.
func (g *Generation) MarkCompletionStart(t time.Time) {
	if g == nil || g.manager == nil || !g.manager.Enabled() || g.span == nil {
		return
	}
	g.span.SetAttributes(attribute.String(attrObsCompletionStart, isoTime(t)))
}

// mergeMetadata combines the metadata captured when an observation opened
// with the metadata supplied at Finish. Finish keys win on conflict (they
// reflect the final outcome), while open-time keys (correlation fields such
// as request_id / http.method) are preserved. Returns nil when both inputs
// are empty so callers can skip writing an empty attribute.
func mergeMetadata(start, finish map[string]interface{}) map[string]interface{} {
	if len(start) == 0 && len(finish) == 0 {
		return nil
	}
	merged := make(map[string]interface{}, len(start)+len(finish))
	for k, v := range start {
		merged[k] = v
	}
	for k, v := range finish {
		merged[k] = v
	}
	return merged
}

// jsonAttr serializes v to a compact JSON string and wraps it as a string
// OTel attribute — matching how langfuse-python stores structured fields
// (input/output/metadata/usage) on spans. nil/zero values return an empty
// KeyValue (harmless on SetAttributes).
func jsonAttr(key string, v interface{}) attribute.KeyValue {
	if v == nil {
		return attribute.KeyValue{Key: attribute.Key(key)}
	}
	b, err := json.Marshal(v)
	if err != nil {
		logger.Warnf(context.Background(), "[Langfuse] marshal attr %s failed: %v", key, err)
		return attribute.KeyValue{Key: attribute.Key(key)}
	}
	if len(b) == 0 || string(b) == "null" {
		// Optional structured fields are often unset; omit rather than warn.
		return attribute.KeyValue{Key: attribute.Key(key)}
	}
	return attribute.String(key, string(b))
}
