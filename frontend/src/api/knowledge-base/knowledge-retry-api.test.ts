import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./index.ts', import.meta.url), 'utf8')

test('keeps row and aggregate retry endpoints distinct and idempotency-keyed', () => {
  assert.match(source, /attempts\/\$\{attempt\}\/spans\/\$\{encodeURIComponent\(spanId\)\}\/retry/)
  assert.match(source, /attempts\/\$\{attempt\}\/retry-failed/)
  assert.match(source, /data: \{ client_request_id: string \}/)
  assert.match(source, /new_attempt: number/)
  assert.match(source, /source_span_id: string/)
  assert.match(source, /target_name: string/)
  assert.match(source, /new_span_id: string/)
  assert.match(source, /task_id: string/)
})
