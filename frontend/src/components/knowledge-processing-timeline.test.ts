import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const source = readFileSync(join(here, 'knowledge-processing-timeline.vue'), 'utf8')

test('counts skipped stages when determining the current stage', () => {
  const currentStageIndex = source.slice(
    source.indexOf('const currentStageIndex'),
    source.indexOf('function formatDuration'),
  )

  assert.match(currentStageIndex, /status === 'done' \|\| s\.status === 'skipped'/)
  assert.match(currentStageIndex, /Math\.min\(traversed \+ 1, stages\.value\.length\)/)
})

test('counts skipped stages in the completed stage total', () => {
  const stagesStatDisplay = source.slice(
    source.indexOf('const stagesStatDisplay'),
    source.indexOf('const postprocessTaskStats'),
  )

  assert.match(stagesStatDisplay, /status === 'done' \|\| s\.status === 'skipped'/)
  assert.match(stagesStatDisplay, /value: `\$\{completedCount\}\/\$\{total\}`/)
})

test('rebuild retry reports acceptance or failure and prevents duplicate clicks', () => {
  const retryHandler = source.slice(
    source.indexOf('async function onRetry'),
    source.indexOf('async function onManualRefresh'),
  )

  assert.match(retryHandler, /if \(mutationBusy\.value\) return/)
  assert.match(retryHandler, /retrying\.value = true/)
  assert.match(retryHandler, /MessagePlugin\.success\(t\('knowledgeBase\.rebuildSubmitted'\)\)/)
  assert.match(retryHandler, /MessagePlugin\.error\(error\?\.message \|\| t\('knowledgeBase\.rebuildFailed'\)\)/)
  assert.match(retryHandler, /finally\s*{\s*retrying\.value = false/)
  assert.match(source, /:loading="retrying"/)
})

test('single-item retry is permission-gated, backend-authorized, and opens a new attempt', () => {
  assert.match(source, /canEdit\?: boolean/)
  assert.match(source, /canRetryKnowledgeSpan\(row\.node, props\.canEdit\)/)
  assert.match(source, /retryKnowledgeSpan\(/)
  assert.match(source, /client_request_id/)
  assert.match(source, /focusAcceptedRepair\(result\.new_attempt/)
  assert.match(source, /retryingSpanIds/)
  assert.match(source, /hasRetryingSpan/)
  assert.match(source, /const mutationBusy = computed/)
  assert.match(source, /retryingFailedItems\.value \|\| cancelling\.value \|\| hasRetryingSpan\.value/)
  assert.match(source, /if \(!spanId \|\| attempt <= 0 \|\| mutationBusy\.value/)
  assert.match(source, /:disabled="mutationBusy"/)
  assert.match(source, /knowledgeStages\.retryItem/)
  assert.match(source, /v-if="props\.canEdit && data\?\.parse_status === 'failed'"/)
  assert.match(source, /v-if="props\.canEdit && canCancelParse"/)
})

test('aggregate retry consumes the object wire contract and focuses the accepted attempt', () => {
  assert.match(source, /retry_failed_action\?: KnowledgeRetryFailedAction/)
  assert.match(source, /retryFailedKnowledgeItems\(/)
  assert.match(source, /target => target\.target_name/)
  assert.match(source, /target => target\.new_span_id/)
  assert.match(source, /findKnowledgeRetryTargetSpanId\(data\.value\?\.trace, targets\)/)
  assert.match(source, /result\.targets\.length === 0/)
  assert.match(source, /retryFailedTargetCount/)
  assert.match(source, /retryFailedCountsText/)
  assert.match(source, /knowledgeStages\.retryFailedItemsConfirm/)
  assert.match(source, /knowledgeStages\.retryFailedItemsSubmitted/)
})

test('retry errors are actionable and refresh conflicting or unpublished attempts', () => {
  assert.match(source, /knowledgeRetryErrorMessageKey\(error\?\.status\)/)
  assert.match(source, /error\?\.status !== 409 && error\?\.status !== 503/)
  assert.match(source, /selectedAttempt\.value = undefined/)
  assert.match(source, /await fetchSpans\(\{ manual: true \}\)/)
})

test('retry controls are keyboard reachable, 36px minimum, themed, and escape the drawer stack', () => {
  assert.match(source, /role="button" tabindex="0"/)
  assert.match(source, /@keydown\.enter\.self\.prevent="selectRow\(row\)"/)
  assert.match(source, /@keydown\.space\.self\.prevent="selectRow\(row\)"/)
  assert.match(source, /\.kp-row:focus-visible/)
  assert.match(source, /\.kp-row-retry:focus-visible/)
  assert.match(source, /min-height: 36px/)
  assert.match(source, /width: 36px;\s*height: 36px/)
  assert.match(source, /:popup-props="\{ attach: 'body', zIndex: 2300 \}"/)
  assert.match(source, /:global\(:root\[theme-mode='light'\]\)/)
  assert.match(source, /:global\(:root\[theme-mode='dark'\]\)/)
  assert.match(source, /var\(--td-bg-color-container\)/)
  assert.match(source, /@media \(max-width: 760px\)/)
})

test('uses attempt-aware live state and terminal duration fallback', () => {
  assert.match(source, /isKnowledgeTraceClockLive\(/)
  assert.match(source, /resolvedKnowledgeSpanDurationMs\(/)
  assert.match(source, /function isNodeClockLive\(node: SpanNode\)/)
  assert.match(source, /const liveBar = isNodeClockLive\(node\)/)
  assert.doesNotMatch(source, /v-if="row\.node\.status === 'running'"/)
  assert.doesNotMatch(source, /v-if="selectedRow\.node\.status === 'running'"/)
})
