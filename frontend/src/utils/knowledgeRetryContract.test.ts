import assert from 'node:assert/strict'
import test from 'node:test'

import {
  findKnowledgeRetryTargetSpanId,
  isKnowledgeRetryFailedActionUsable,
  type KnowledgeRetryFailedAction,
  type KnowledgeTraceNode,
} from './knowledgeTrace.ts'

const spansFixture = JSON.parse(`{
  "success": true,
  "data": {
    "attempt": 8,
    "latest_attempt": 8,
    "retry_failed_action": {
      "allowed": true,
      "counts": {"summary": 1, "wiki": 1, "graph": 1, "question": 1},
      "targets": [
        {"source_span_id": "summary-old", "target_name": "postprocess.summary", "state": "failed"},
        {"source_span_id": "wiki-old", "target_name": "postprocess.wiki", "state": "stalled"},
        {"source_span_id": "graph-old", "target_name": "postprocess.graph.chunk[7]", "state": "failed"},
        {"source_span_id": "question-old", "target_name": "postprocess.question.batch[3]", "state": "failed"}
      ]
    }
  }
}`) as { data: { retry_failed_action: KnowledgeRetryFailedAction } }

const acceptedFixture = JSON.parse(`{
  "success": true,
  "data": {
    "knowledge_id": "kid",
    "source_attempt": 8,
    "client_request_id": "request-1",
    "new_attempt": 9,
    "targets": [
      {
        "source_span_id": "wiki-old",
        "target_name": "postprocess.wiki",
        "state": "stalled",
        "new_span_id": "wiki-new",
        "task_id": "knowledge-fanout:kid:9:wiki"
      },
      {
        "source_span_id": "question-old",
        "target_name": "postprocess.question.batch[3]",
        "state": "failed",
        "new_span_id": "question-new",
        "task_id": "knowledge-fanout:kid:9:question:3"
      }
    ]
  }
}`) as {
  data: {
    new_attempt: number
    targets: Array<{
      source_span_id: string
      target_name: string
      state: string
      new_span_id: string
      task_id: string
    }>
  }
}

test('GET spans aggregate action preserves exact category counts and object targets', () => {
  const action = spansFixture.data.retry_failed_action
  const targets = action.targets || []
  assert.equal(action.allowed, true)
  assert.deepEqual(action.counts, { summary: 1, wiki: 1, graph: 1, question: 1 })
  assert.deepEqual(targets.map(target => target.target_name), [
    'postprocess.summary',
    'postprocess.wiki',
    'postprocess.graph.chunk[7]',
    'postprocess.question.batch[3]',
  ])
  assert.equal(targets[1]?.state, 'stalled')
  assert.equal(isKnowledgeRetryFailedActionUsable(action), true)
  assert.equal(isKnowledgeRetryFailedActionUsable({
    ...action,
    targets: [{ source_span_id: 'root', target_name: 'knowledge_processing', state: 'failed' }],
  }), false)
  assert.equal(isKnowledgeRetryFailedActionUsable({
    ...action,
    counts: { ...action.counts, graph: 2 },
  }), false)
})

test('202 aggregate response carries new span and task identity for each exact target', () => {
  assert.equal(acceptedFixture.data.new_attempt, 9)
  assert.deepEqual(acceptedFixture.data.targets.map(target => ({
    target: target.target_name,
    newSpan: target.new_span_id,
    task: target.task_id,
  })), [
    { target: 'postprocess.wiki', newSpan: 'wiki-new', task: 'knowledge-fanout:kid:9:wiki' },
    { target: 'postprocess.question.batch[3]', newSpan: 'question-new', task: 'knowledge-fanout:kid:9:question:3' },
  ])
})

test('202 focus uses a returned new_span_id before target-name fallback', () => {
  const trace: KnowledgeTraceNode = {
    name: 'knowledge_processing',
    kind: 'root',
    status: 'running',
    children: [{
      name: 'postprocess',
      kind: 'stage',
      status: 'running',
      children: [{
        span_id: 'wiki-fallback',
        name: 'postprocess.wiki',
        kind: 'subspan',
        status: 'pending',
      }],
    }],
  }
  const preferred = acceptedFixture.data.targets.find(target => target.new_span_id)?.new_span_id
  assert.equal(preferred, 'wiki-new')
  assert.equal(findKnowledgeRetryTargetSpanId(trace, acceptedFixture.data.targets), 'wiki-fallback')
})
