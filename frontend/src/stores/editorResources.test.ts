import assert from 'node:assert/strict'
import test from 'node:test'

import { createLatestKeyedRequestCoordinator } from './editorResourcesCoordinator.ts'

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

test('switching skill configs queues the new config and suppresses the stale result', async () => {
  const configA = deferred<string>()
  const configB = deferred<string>()
  const requests = new Map([
    ['config-a', configA.promise],
    ['config-b', configB.promise],
  ])
  const started: string[] = []
  const applied: Array<[string, string]> = []
  const coordinator = createLatestKeyedRequestCoordinator(
    (configId: string) => {
      started.push(configId)
      return requests.get(configId)!
    },
    (configId, value) => applied.push([configId, value]),
  )

  const requestA = coordinator.fetch('config-a')
  const requestB = coordinator.fetch('config-b')
  assert.deepEqual(started, ['config-a'])

  configA.resolve('skills-a')
  await requestA
  await new Promise<void>((resolve) => queueMicrotask(resolve))
  assert.deepEqual(started, ['config-a', 'config-b'])
  assert.deepEqual(applied, [])

  configB.resolve('skills-b')
  await requestB
  assert.deepEqual(applied, [['config-b', 'skills-b']])
})

test('repeated reads of the latest key share its queued request', async () => {
  const result = deferred<string>()
  let calls = 0
  const coordinator = createLatestKeyedRequestCoordinator(
    async () => {
      calls += 1
      return result.promise
    },
    () => {},
  )

  const first = coordinator.fetch('config-a')
  const second = coordinator.fetch('config-a')

  assert.equal(first, second)
  assert.equal(calls, 1)
  result.resolve('skills-a')
  await first
})

test('invalidate makes the next read queue a fresh request for the same key', async () => {
  const first = deferred<string>()
  const second = deferred<string>()
  const requests = [first, second]
  const applied: string[] = []
  let calls = 0
  const coordinator = createLatestKeyedRequestCoordinator(
    () => requests[calls++].promise,
    (_key, value) => applied.push(value),
  )

  const stale = coordinator.fetch('config-a')
  coordinator.invalidate()
  const fresh = coordinator.fetch('config-a')

  first.resolve('stale')
  await stale
  await new Promise<void>((resolve) => queueMicrotask(resolve))
  assert.equal(calls, 2)

  second.resolve('fresh')
  await fresh
  assert.deepEqual(applied, ['fresh'])
})
