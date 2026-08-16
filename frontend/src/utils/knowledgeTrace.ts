/** Whether GET /knowledge/:id/spans returned a real trace (not legacy placeholder-only). */
export function knowledgeSpansPayloadHasTrace(
  data: { trace?: { span_id?: string }; current_attempt?: number } | null | undefined,
): boolean {
  if (!data?.trace) return false
  return !!(data.trace.span_id || (data.current_attempt ?? 0) > 0)
}

export interface KnowledgeTraceNode {
  span_id?: string
  parent_span_id?: string
  name: string
  kind: string
  status: string
  started_at?: string | null
  finished_at?: string | null
  duration_ms?: number
  error_code?: string
  error_message?: string
  input?: unknown
  output?: unknown
  metadata?: unknown
  retry_action?: KnowledgeSpanRetryAction
  children?: KnowledgeTraceNode[]
}

export interface KnowledgeSpanRetryAction {
  allowed: boolean
  target?: string
  state?: 'failed' | 'stalled' | 'active' | 'unknown' | string
  reason?: string
}

export interface KnowledgeRetryFailedCounts {
  summary: number
  wiki: number
  graph: number
  question: number
}

export interface KnowledgeRetryTarget {
  source_span_id: string
  target_name: string
  state: 'failed' | 'stalled' | string
}

export interface KnowledgeAcceptedRetryTarget extends KnowledgeRetryTarget {
  new_span_id: string
  task_id: string
}

export interface KnowledgeRetryFailedAction {
  allowed: boolean
  reason?: string
  counts: KnowledgeRetryFailedCounts
  targets?: KnowledgeRetryTarget[]
}

const retryableKnowledgeOwnerPatterns = [
  /^postprocess\.summary$/,
  /^postprocess\.wiki$/,
  /^postprocess\.graph\.chunk\[\d+\]$/,
  /^postprocess\.question\.batch\[\d+\]$/,
]

export function isRetryableKnowledgeOwnerName(value: unknown): value is string {
  return typeof value === 'string' && retryableKnowledgeOwnerPatterns.some(pattern => pattern.test(value))
}

function retryTargetCategory(targetName: string): keyof KnowledgeRetryFailedCounts | null {
  if (targetName === 'postprocess.summary') return 'summary'
  if (targetName === 'postprocess.wiki') return 'wiki'
  if (/^postprocess\.graph\.chunk\[\d+\]$/.test(targetName)) return 'graph'
  if (/^postprocess\.question\.batch\[\d+\]$/.test(targetName)) return 'question'
  return null
}

export function isKnowledgeRetryFailedActionUsable(
  action: KnowledgeRetryFailedAction | null | undefined,
): action is KnowledgeRetryFailedAction {
  if (!action?.allowed || !Array.isArray(action.targets) || action.targets.length === 0) return false
  const actual: KnowledgeRetryFailedCounts = { summary: 0, wiki: 0, graph: 0, question: 0 }
  for (const target of action.targets) {
    const category = retryTargetCategory(target?.target_name)
    if (!target?.source_span_id || !category || (target.state !== 'failed' && target.state !== 'stalled')) {
      return false
    }
    actual[category] += 1
  }
  return (Object.keys(actual) as Array<keyof KnowledgeRetryFailedCounts>).every(key =>
    Number(action.counts?.[key]) === actual[key],
  )
}

/**
 * The backend owns retry eligibility because it can verify the latest attempt,
 * immutable fan-out plan, and queue owner. The client only adds the local edit
 * permission and failed/span-id guards needed to avoid presenting a dead 403.
 */
export function canRetryKnowledgeSpan(node: KnowledgeTraceNode | null | undefined, canEdit: boolean): boolean {
  if (!canEdit || !node?.span_id || node.kind !== 'subspan' || node.retry_action?.allowed !== true) return false
  const target = node.retry_action.target
  if (!isRetryableKnowledgeOwnerName(node.name) || target !== node.name) return false
  if (node.retry_action.state === 'failed') return node.status === 'failed'
  if (node.retry_action.state === 'stalled') return node.status === 'running' || node.status === 'pending'
  return false
}

export function knowledgeRetryErrorMessageKey(status: unknown): string {
  if (status === 403) return 'knowledgeStages.retryError.forbidden'
  if (status === 409) return 'knowledgeStages.retryError.conflict'
  if (status === 503) return 'knowledgeStages.retryError.unavailable'
  return 'knowledgeStages.retryError.generic'
}

export function findKnowledgeRetryTargetSpanId(
  root: KnowledgeTraceNode | null | undefined,
  targets: ReadonlyArray<Pick<KnowledgeRetryTarget, 'target_name'>>,
): string | null {
  if (!root || targets.length === 0) return null
  const targetSet = new Set(targets.map(target => target.target_name).filter(isRetryableKnowledgeOwnerName))
  let fallback: string | null = null
  const visit = (node: KnowledgeTraceNode): string | null => {
    if (targetSet.has(node.name) && node.span_id) {
      if (node.status === 'pending' || node.status === 'running') return node.span_id
      fallback ||= node.span_id
    }
    for (const child of node.children || []) {
      const matched = visit(child)
      if (matched) return matched
    }
    return null
  }
  return visit(root) || fallback
}

export interface PostprocessTaskSummary {
  running: number
  failed: number
  completed: number
  other: number
  total: number
}

export type StreamTransportMilestoneKey =
  | 'created'
  | 'request_dispatched'
  | 'stream_opened'
  | 'first_sse_event'
  | 'completed'

export interface StreamTransportMilestone {
  key: StreamTransportMilestoneKey
  reached: boolean
  active: boolean
  at?: string
}

export interface StreamTransportProgress {
  state: string
  protocol?: string
  endpoint?: string
  firstEventType?: string
  waitingForCompletion: boolean
  outcome?: 'done' | 'failed' | 'cancelled'
  milestones: StreamTransportMilestone[]
}

export interface KnowledgeTraceClockState {
  attempt?: number
  latestAttempt?: number
  parseStatus?: string
  traceActive?: boolean
}

const liveParseStatuses = new Set(['pending', 'processing', 'finalizing'])
const terminalParseStatuses = new Set(['cancelled', 'failed', 'completed'])
const terminalSpanStatuses = new Set(['done', 'failed', 'cancelled', 'skipped'])

/**
 * Historical attempts are immutable even when the knowledge row currently
 * describes a newer attempt that is still processing. This helper keeps the
 * live clock bound to the selected attempt instead of the shared knowledge
 * status returned alongside it.
 */
export function isKnowledgeTraceClockLive(state: KnowledgeTraceClockState): boolean {
  const attempt = state.attempt ?? 0
  const latestAttempt = state.latestAttempt ?? 0
  if (attempt > 0 && latestAttempt > 0 && attempt < latestAttempt) return false
  if (terminalParseStatuses.has(state.parseStatus || '')) return false
  return liveParseStatuses.has(state.parseStatus || '') || state.traceActive === true
}

/**
 * Old cancellation/supersede rows may have finished_at but duration_ms=0.
 * Derive their frozen elapsed time for display without mutating history.
 */
export function resolvedKnowledgeSpanDurationMs(
  node: Pick<KnowledgeTraceNode, 'status' | 'started_at' | 'finished_at' | 'duration_ms'>,
): number | undefined {
  if (typeof node.duration_ms === 'number' && node.duration_ms > 0) {
    return node.duration_ms
  }
  if (!terminalSpanStatuses.has(node.status)) {
    return typeof node.duration_ms === 'number' && node.duration_ms >= 0
      ? node.duration_ms
      : undefined
  }
  const started = timestamp(node.started_at)
  const finished = timestamp(node.finished_at)
  if (started !== null && finished !== null) {
    return Math.max(0, finished - started)
  }
  return typeof node.duration_ms === 'number' && node.duration_ms >= 0
    ? node.duration_ms
    : undefined
}

function objectRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Record<string, unknown>
}

/**
 * Older generation rows stored an output containing only an unavailable usage
 * shell. That object is observability metadata, not model content, so the
 * regular Output tab must not present it as a successful LLM answer. Raw JSON
 * remains untouched for historical diagnosis.
 */
export function isUnavailableUsageShell(value: unknown): boolean {
  const output = objectRecord(value)
  if (!output || Object.keys(output).length !== 1) return false
  const usage = objectRecord(output.usage)
  return usage?.available === false
}

export function visibleKnowledgeSpanOutput(value: unknown): unknown {
  return isUnavailableUsageShell(value) ? null : value
}

/** Derives a stable transport timeline from backend generation metadata. */
export function getStreamTransportProgress(
  node: Pick<KnowledgeTraceNode, 'status' | 'metadata'>,
): StreamTransportProgress | null {
  const metadata = objectRecord(node.metadata)
  const progress = objectRecord(metadata?.stream_progress)
  if (!progress) return null

  const state = typeof progress.state === 'string' ? progress.state : 'created'
  const outcome = node.status === 'done' || node.status === 'failed' || node.status === 'cancelled'
    ? node.status
    : undefined
  const keys: StreamTransportMilestoneKey[] = [
    'created',
    'request_dispatched',
    'stream_opened',
    'first_sse_event',
    'completed',
  ]
  const reached = new Set<StreamTransportMilestoneKey>()
  for (const key of keys.slice(0, -1)) {
    if (typeof progress[`${key}_at`] === 'string') reached.add(key)
  }
  if (outcome === 'done') reached.add('completed')

  const firstUnreached = keys.find(key => !reached.has(key))
  const milestones = keys.map(key => ({
    key,
    reached: reached.has(key),
    active: outcome === undefined && key === firstUnreached,
    at: typeof progress[`${key}_at`] === 'string'
      ? progress[`${key}_at`] as string
      : undefined,
  }))

  return {
    state,
    protocol: typeof progress.protocol === 'string' ? progress.protocol : undefined,
    endpoint: typeof progress.endpoint === 'string' ? progress.endpoint : undefined,
    firstEventType: typeof progress.first_event_type === 'string' ? progress.first_event_type : undefined,
    waitingForCompletion: outcome === undefined && reached.has('first_sse_event'),
    outcome,
    milestones,
  }
}

const graphChunkName = /^postprocess\.graph\.chunk\[(\d+)\]$/

function timestamp(value?: string | null): number | null {
  if (!value) return null
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? null : parsed
}

function nodeEnd(node: KnowledgeTraceNode): number | null {
  const finished = timestamp(node.finished_at)
  if (finished !== null) return finished
  const started = timestamp(node.started_at)
  const duration = resolvedKnowledgeSpanDurationMs(node)
  if (started !== null && duration !== undefined) {
    return started + duration
  }
  return null
}

function aggregateStatus(nodes: KnowledgeTraceNode[]): string {
  if (nodes.some(node => node.status === 'running' || node.status === 'pending')) return 'running'
  if (nodes.some(node => node.status === 'failed')) return 'failed'
  if (nodes.every(node => node.status === 'skipped')) return 'skipped'
  if (nodes.some(node => node.status === 'cancelled')) return 'cancelled'
  return 'done'
}

/**
 * Groups persisted postprocess.graph.chunk[i] spans into one derived graph
 * node. The derived duration is wall-clock time from the first graph worker
 * start to the final graph worker finish; children retain per-chunk detail.
 */
export function groupPostprocessGraphSpans(
  stage: KnowledgeTraceNode,
): KnowledgeTraceNode {
  const children = stage.children || []
  const graphHistoryChildren = children.filter(child => graphChunkName.test(child.name))
  if (graphHistoryChildren.length === 0) return stage

  const latestGraphChildren = new Map<string, KnowledgeTraceNode>()
  for (const child of graphHistoryChildren) {
    // Spans are returned in creation order. Retried chunks keep the same
    // name, so replacing the entry leaves only the latest logical attempt.
    latestGraphChildren.set(child.name, child)
  }
  const graphChildren = [...latestGraphChildren.values()]

  const starts = graphChildren
    .map(child => timestamp(child.started_at))
    .filter((value): value is number => value !== null)
  const ends = graphChildren
    .map(nodeEnd)
    .filter((value): value is number => value !== null)
  const status = aggregateStatus(graphChildren)
  const start = starts.length > 0 ? Math.min(...starts) : null
  const terminal = status !== 'running'
  const end = terminal && ends.length > 0 ? Math.max(...ends) : null
  const counts = graphChildren.reduce<Record<string, number>>((result, child) => {
    result[child.status] = (result[child.status] || 0) + 1
    return result
  }, {})

  const group: KnowledgeTraceNode = {
    span_id: `virtual:postprocess.graph:${stage.span_id || 'stage'}`,
    parent_span_id: stage.span_id,
    name: 'postprocess.graph',
    kind: 'group',
    status,
    started_at: start === null ? null : new Date(start).toISOString(),
    finished_at: end === null ? null : new Date(end).toISOString(),
    duration_ms: start !== null && end !== null ? Math.max(0, end - start) : undefined,
    input: { chunk_count: graphChildren.length },
    output: { chunk_count: graphChildren.length, status_counts: counts },
    // Preserve superseded attempts in the expandable history. Only the
    // aggregate status and counts above use the latest logical attempt.
    children: graphHistoryChildren,
  }

  let inserted = false
  const groupedChildren: KnowledgeTraceNode[] = []
  for (const child of children) {
    if (graphChunkName.test(child.name)) {
      if (!inserted) {
        groupedChildren.push(group)
        inserted = true
      }
      continue
    }
    groupedChildren.push(child)
  }

  return { ...stage, children: groupedChildren }
}

/**
 * Counts leaf postprocess spans so the UI can distinguish the five main
 * pipeline stages from asynchronous enrichment work.
 */
export function summarizePostprocessTasks(
  trace?: KnowledgeTraceNode,
): PostprocessTaskSummary {
  const summary: PostprocessTaskSummary = {
    running: 0,
    failed: 0,
    completed: 0,
    other: 0,
    total: 0,
  }
  if (!trace) return summary

  const postprocess = trace.name === 'postprocess'
    ? trace
    : (trace.children || []).find(child => child.name === 'postprocess')
  if (!postprocess) return summary

  const countLeaves = (node: KnowledgeTraceNode) => {
    const children = node.children || []
    if (children.length > 0) {
      children.forEach(countLeaves)
      return
    }

    summary.total++
    switch (node.status) {
      case 'running':
      case 'pending':
      case 'processing':
      case 'finalizing':
        summary.running++
        break
      case 'failed':
        summary.failed++
        break
      case 'done':
      case 'completed':
        summary.completed++
        break
      default:
        summary.other++
    }
  }

  ;(postprocess.children || []).forEach(countLeaves)
  return summary
}
