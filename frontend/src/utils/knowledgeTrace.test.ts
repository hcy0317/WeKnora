import assert from 'node:assert/strict'
import test from 'node:test'

import {
  canRetryKnowledgeSpan,
  findKnowledgeRetryTargetSpanId,
  getStreamTransportProgress,
  groupPostprocessGraphSpans,
  isKnowledgeTraceClockLive,
  isUnavailableUsageShell,
  knowledgeRetryErrorMessageKey,
  resolvedKnowledgeSpanDurationMs,
  summarizePostprocessTasks,
  visibleKnowledgeSpanOutput,
  type KnowledgeTraceNode,
} from './knowledgeTrace.ts'

test('only exposes backend-approved failed or stalled exact owners to editors', () => {
  const failedWiki: KnowledgeTraceNode = {
    span_id: 'wiki-failed',
    name: 'postprocess.wiki',
    kind: 'subspan',
    status: 'failed',
    retry_action: {
      allowed: true,
      target: 'postprocess.wiki',
      state: 'failed',
    },
  }

  assert.equal(canRetryKnowledgeSpan(failedWiki, true), true)
  assert.equal(canRetryKnowledgeSpan(failedWiki, false), false)
  assert.equal(canRetryKnowledgeSpan({
    ...failedWiki,
    status: 'running',
    retry_action: { allowed: true, target: 'postprocess.wiki', state: 'stalled' },
  }, true), true)
  assert.equal(canRetryKnowledgeSpan({
    ...failedWiki,
    name: 'postprocess.question.batch[3]',
    retry_action: { allowed: true, target: 'postprocess.question.batch[3]', state: 'failed' },
  }, true), true)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, status: 'running' }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, retry_action: { allowed: false, reason: 'historical_attempt' } }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, span_id: '' }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, status: 'done' }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, status: 'cancelled' }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, name: 'knowledge_processing' }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, kind: 'stage', name: 'postprocess.summary' }, true), false)
  assert.equal(canRetryKnowledgeSpan({ ...failedWiki, name: 'postprocess.wiki.page[0]' }, true), false)
  assert.equal(canRetryKnowledgeSpan({
    ...failedWiki,
    retry_action: { allowed: true, target: 'postprocess.summary', state: 'failed' },
  }, true), false)
})

test('maps retry API conflicts to actionable UI messages', () => {
  assert.equal(knowledgeRetryErrorMessageKey(403), 'knowledgeStages.retryError.forbidden')
  assert.equal(knowledgeRetryErrorMessageKey(409), 'knowledgeStages.retryError.conflict')
  assert.equal(knowledgeRetryErrorMessageKey(503), 'knowledgeStages.retryError.unavailable')
  assert.equal(knowledgeRetryErrorMessageKey(500), 'knowledgeStages.retryError.generic')
})

test('focuses a pending exact target in a newly accepted partial repair attempt', () => {
  const root: KnowledgeTraceNode = {
    name: 'knowledge_processing',
    kind: 'root',
    status: 'running',
    children: [{
      name: 'postprocess',
      kind: 'stage',
      status: 'running',
      children: [
        { span_id: 'wiki-done', name: 'postprocess.wiki', kind: 'subspan', status: 'done' },
        { span_id: 'graph-pending', name: 'postprocess.graph.chunk[7]', kind: 'subspan', status: 'pending' },
        { span_id: 'diagnostic', name: 'postprocess.wiki.page[1]', kind: 'subspan', status: 'pending' },
      ],
    }],
  }

  assert.equal(findKnowledgeRetryTargetSpanId(root, [
    { target_name: 'postprocess.wiki' },
    { target_name: 'postprocess.graph.chunk[7]' },
    { target_name: 'postprocess.wiki.page[1]' },
  ]), 'graph-pending')
  assert.equal(findKnowledgeRetryTargetSpanId(root, [{ target_name: 'postprocess.wiki.page[1]' }]), null)
})

test('freezes historical attempts even while the latest attempt is processing', () => {
  assert.equal(isKnowledgeTraceClockLive({
    attempt: 3,
    latestAttempt: 8,
    parseStatus: 'processing',
    traceActive: true,
  }), false)

  assert.equal(isKnowledgeTraceClockLive({
    attempt: 8,
    latestAttempt: 8,
    parseStatus: 'processing',
    traceActive: true,
  }), true)
})

test('derives frozen terminal duration from finished_at for legacy zero-duration rows', () => {
  assert.equal(resolvedKnowledgeSpanDurationMs({
    status: 'cancelled',
    started_at: '2026-08-09T12:47:36.000Z',
    finished_at: '2026-08-09T12:55:09.000Z',
    duration_ms: 0,
  }), 453000)
})

function graphChunk(index: number, overrides: Partial<KnowledgeTraceNode> = {}): KnowledgeTraceNode {
  return {
    span_id: `graph-${index}`,
    parent_span_id: 'postprocess',
    name: `postprocess.graph.chunk[${index}]`,
    kind: 'subspan',
    status: 'done',
    started_at: `2026-07-21T08:00:0${index}.000Z`,
    finished_at: `2026-07-21T08:00:0${index + 2}.000Z`,
    duration_ms: 2000,
    ...overrides,
  }
}

test('groups graph chunks and reports their wall-clock duration', () => {
  const summary: KnowledgeTraceNode = {
    span_id: 'summary',
    name: 'postprocess.summary',
    kind: 'subspan',
    status: 'done',
  }
  const stage: KnowledgeTraceNode = {
    span_id: 'postprocess',
    name: 'postprocess',
    kind: 'stage',
    status: 'done',
    children: [summary, graphChunk(0), graphChunk(1)],
  }

  const grouped = groupPostprocessGraphSpans(stage)
  assert.equal(grouped.children?.length, 2)
  assert.equal(grouped.children?.[0], summary)

  const graph = grouped.children?.[1]
  assert.equal(graph?.name, 'postprocess.graph')
  assert.equal(graph?.status, 'done')
  assert.equal(graph?.duration_ms, 3000)
  assert.equal(graph?.children?.length, 2)
  assert.deepEqual(graph?.output, {
    chunk_count: 2,
    status_counts: { done: 2 },
  })
})

test('keeps graph group live while any graph chunk is running', () => {
  const stage: KnowledgeTraceNode = {
    span_id: 'postprocess',
    name: 'postprocess',
    kind: 'stage',
    status: 'done',
    children: [
      graphChunk(0),
      graphChunk(1, { status: 'running', finished_at: null, duration_ms: undefined }),
    ],
  }

  const graph = groupPostprocessGraphSpans(stage).children?.[0]
  assert.equal(graph?.status, 'running')
  assert.equal(graph?.finished_at, null)
  assert.equal(graph?.duration_ms, undefined)
})

test('surfaces a failed graph chunk on the aggregate graph row', () => {
  const stage: KnowledgeTraceNode = {
    span_id: 'postprocess',
    name: 'postprocess',
    kind: 'stage',
    status: 'done',
    children: [graphChunk(0), graphChunk(1, { status: 'failed' })],
  }

  const graph = groupPostprocessGraphSpans(stage).children?.[0]
  assert.equal(graph?.status, 'failed')
  assert.equal(graph?.duration_ms, 3000)
})

test('keeps the aggregate running until all graph chunks are terminal', () => {
  const stage: KnowledgeTraceNode = {
    span_id: 'postprocess',
    name: 'postprocess',
    kind: 'stage',
    status: 'done',
    children: [
      graphChunk(0, { status: 'failed' }),
      graphChunk(1, { status: 'running', finished_at: null, duration_ms: undefined }),
    ],
  }

  const graph = groupPostprocessGraphSpans(stage).children?.[0]
  assert.equal(graph?.status, 'running')
  assert.equal(graph?.duration_ms, undefined)
})

test('uses only the latest retry for each graph chunk', () => {
  const superseded = graphChunk(0, {
    span_id: 'graph-0-old',
    status: 'cancelled',
    error_code: 'TASK_SUPERSEDED',
    error_message: 'superseded by a new run of the same subtask',
  })
  const completedRetry = graphChunk(0, {
    span_id: 'graph-0-retry',
    started_at: '2026-07-21T08:00:03.000Z',
    finished_at: '2026-07-21T08:00:05.000Z',
  })
  const stage: KnowledgeTraceNode = {
    span_id: 'postprocess',
    name: 'postprocess',
    kind: 'stage',
    status: 'done',
    children: [superseded, graphChunk(1), completedRetry],
  }

  const graph = groupPostprocessGraphSpans(stage).children?.[0]
  assert.equal(graph?.status, 'done')
  assert.equal(graph?.children?.length, 3)
  assert.deepEqual(graph?.children?.map(child => child.span_id), [
    'graph-0-old',
    'graph-1',
    'graph-0-retry',
  ])
  assert.deepEqual(graph?.output, {
    chunk_count: 2,
    status_counts: { done: 2 },
  })
})

test('leaves postprocess unchanged when it has no graph chunks', () => {
  const stage: KnowledgeTraceNode = {
    span_id: 'postprocess',
    name: 'postprocess',
    kind: 'stage',
    status: 'done',
    children: [],
  }

  assert.equal(groupPostprocessGraphSpans(stage), stage)
})

test('summarizes asynchronous postprocess leaf tasks', () => {
  const trace: KnowledgeTraceNode = {
    name: 'root',
    kind: 'root',
    status: 'done',
    children: [
      {
        name: 'postprocess',
        kind: 'stage',
        status: 'done',
        children: [
          { name: 'postprocess.summary', kind: 'subspan', status: 'done' },
          { name: 'postprocess.question', kind: 'subspan', status: 'running' },
          {
            name: 'postprocess.graph',
            kind: 'group',
            status: 'failed',
            children: [
              { name: 'postprocess.graph.chunk[0]', kind: 'subspan', status: 'failed' },
              { name: 'postprocess.graph.chunk[1]', kind: 'subspan', status: 'pending' },
            ],
          },
        ],
      },
    ],
  }

  assert.deepEqual(summarizePostprocessTasks(trace), {
    running: 2,
    failed: 1,
    completed: 1,
    other: 0,
    total: 4,
  })
})

test('returns an empty postprocess summary when no trace is available', () => {
  assert.deepEqual(summarizePostprocessTasks(undefined), {
    running: 0,
    failed: 0,
    completed: 0,
    other: 0,
    total: 0,
  })
})

test('hides only the legacy unavailable-usage output shell', () => {
  const shell = {
    usage: {
      available: false,
      input_tokens: 0,
      output_tokens: 0,
      total_tokens: 0,
    },
  }
  assert.equal(isUnavailableUsageShell(shell), true)
  assert.equal(visibleKnowledgeSpanOutput(shell), null)
  assert.equal(isUnavailableUsageShell({ content: '', usage: shell.usage }), false)
  assert.deepEqual(visibleKnowledgeSpanOutput({ usage: { available: true } }), {
    usage: { available: true },
  })
})

test('derives Responses transport milestones and waits for completed', () => {
  const progress = getStreamTransportProgress({
    status: 'running',
    metadata: {
      stream_progress: {
        state: 'first_sse_event',
        protocol: 'responses',
        endpoint: 'https://example.test/v1/responses',
        first_event_type: 'response.created',
        created_at: '2026-08-08T00:00:00Z',
        request_dispatched_at: '2026-08-08T00:00:01Z',
        stream_opened_at: '2026-08-08T00:00:02Z',
        first_sse_event_at: '2026-08-08T00:00:03Z',
      },
    },
  })
  assert.equal(progress?.waitingForCompletion, true)
  assert.equal(progress?.protocol, 'responses')
  assert.deepEqual(progress?.milestones.map(step => [step.key, step.reached, step.active]), [
    ['created', true, false],
    ['request_dispatched', true, false],
    ['stream_opened', true, false],
    ['first_sse_event', true, false],
    ['completed', false, true],
  ])
})

test('marks completed only from a terminal successful generation', () => {
  const progress = getStreamTransportProgress({
    status: 'done',
    metadata: { stream_progress: { state: 'first_sse_event', created_at: '2026-08-08T00:00:00Z' } },
  })
  assert.equal(progress?.outcome, 'done')
  assert.equal(progress?.waitingForCompletion, false)
  assert.equal(progress?.milestones.at(-1)?.reached, true)

  const failed = getStreamTransportProgress({
    status: 'failed',
    metadata: { stream_progress: { state: 'stream_opened', created_at: '2026-08-08T00:00:00Z' } },
  })
  assert.equal(failed?.outcome, 'failed')
  assert.equal(failed?.milestones.at(-1)?.reached, false)
})
