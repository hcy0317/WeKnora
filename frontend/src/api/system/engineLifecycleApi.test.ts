import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./index.ts', import.meta.url), 'utf8')

test('SystemAdmin lifecycle API sends the whole editable config with a strong If-Match revision', () => {
  assert.match(source, /export async function getEngineLifecycle/, 'missing lifecycle GET')
  assert.match(source, /export async function updateEngineLifecycle/, 'missing lifecycle PUT')
  assert.match(
    source,
    /\/api\/v1\/system\/admin\/engine-lifecycle/,
    'missing lifecycle SystemAdmin route',
  )
  assert.match(
    source,
    /['"]If-Match['"]\s*:\s*`"\$\{revision\}"`/,
    'missing strong If-Match revision',
  )
})

test('parser engine checks defer timeout enforcement to the lifecycle-aware server probe', () => {
  const start = source.indexOf('export function checkParserEngines')
  const end = source.indexOf('export function getParserEngineConfig', start)

  assert.notEqual(start, -1, 'missing parser engine check API')
  assert.notEqual(end, -1, 'missing parser engine config API boundary')
  assert.match(
    source.slice(start, end),
    /post\([^;]+\{\s*timeout:\s*0\s*\}\)/s,
    'parser engine checks must not inherit the generic 30-second Axios timeout',
  )
})
