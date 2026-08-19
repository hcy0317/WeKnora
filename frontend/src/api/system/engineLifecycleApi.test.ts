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
