import assert from 'node:assert/strict'
import test from 'node:test'

import {
  buildEngineLifecycleUpdate,
  classifyEngineLifecycleError,
  managedEngineGroup,
  type EngineLifecycleConfig,
} from './engineLifecyclePolicy'

const config: EngineLifecycleConfig = {
  controller_online: true,
  observe_only: false,
  revision: 7,
  defaults: {
    idle_minutes: 10,
    startup_timeout_seconds: 120,
    failure_cooldown_minutes: 5,
  },
  groups: {
    paddleocr: { mode: 'on_demand', status: { group: 'paddleocr', state: 'stopped' } },
    asr: { mode: 'always_on', idle_minutes: 4, status: { group: 'asr', state: 'ready' } },
    reranker: { mode: 'on_demand', status: { group: 'reranker', state: 'busy', active: 1 } },
  },
}

test('managedEngineGroup only exposes lifecycle controls for fixed local engines', () => {
  assert.equal(managedEngineGroup('asr', 'http://speaches:8000/v1'), 'asr')
  assert.equal(managedEngineGroup('rerank', 'http://accelerator-router:18083/v1/'), 'reranker')
  assert.equal(managedEngineGroup('rerank', 'https://api.jina.ai/v1'), null)
  assert.equal(managedEngineGroup('asr', 'https://api.openai.com/v1'), null)
  assert.equal(managedEngineGroup('chat', 'http://speaches:8000/v1'), null)
})

test('buildEngineLifecycleUpdate preserves other groups and strips runtime-only fields', () => {
  const payload = buildEngineLifecycleUpdate(config, 'asr', {
    mode: 'on_demand',
    idle_minutes: 2,
    startup_timeout_seconds: 180,
  })

  assert.deepEqual(payload, {
    defaults: { idle_minutes: 10, startup_timeout_seconds: 120 },
    groups: {
      paddleocr: { mode: 'on_demand' },
      asr: { mode: 'on_demand', idle_minutes: 2, startup_timeout_seconds: 180 },
      reranker: { mode: 'on_demand' },
    },
  })
  assert.equal(config.groups.asr.mode, 'always_on')
})

test('classifyEngineLifecycleError distinguishes CAS conflicts from controller outages', () => {
  assert.deepEqual(classifyEngineLifecycleError({ status: 409, revision: 12 }), {
    kind: 'conflict',
    revision: 12,
  })
  assert.deepEqual(classifyEngineLifecycleError({ status: 503 }), {
    kind: 'offline',
  })
  assert.deepEqual(classifyEngineLifecycleError(new Error('bad request')), {
    kind: 'other',
  })
})
