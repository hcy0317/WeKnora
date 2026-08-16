import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { existsSync } from 'node:fs'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { chromium } from 'playwright-core'

const scriptDir = dirname(fileURLToPath(import.meta.url))
const repoRoot = resolve(scriptDir, '..', '..')
const frontendDir = join(repoRoot, 'frontend')
const viteBin = join(frontendDir, 'node_modules', 'vite', 'bin', 'vite.js')
const port = Number(process.env.KNOWLEDGE_RETRY_PREVIEW_PORT || 4179)
const baseURL = `http://127.0.0.1:${port}`
const unmockedApiPaths = new Set()
const chromeCandidates = [
  process.env.CHROME_PATH,
  'C:/Program Files/Google/Chrome/Application/chrome.exe',
  'C:/Program Files (x86)/Google/Chrome/Application/chrome.exe',
  process.env.LOCALAPPDATA
    ? join(process.env.LOCALAPPDATA, 'Google', 'Chrome', 'Application', 'chrome.exe')
    : '',
].filter(Boolean)

function run(command, args, cwd) {
  return new Promise((resolveRun, rejectRun) => {
    const child = spawn(command, args, { cwd, stdio: 'inherit', windowsHide: true })
    child.once('error', rejectRun)
    child.once('exit', (code, signal) => {
      if (code === 0) resolveRun()
      else rejectRun(new Error(`${command} exited with code=${code} signal=${signal || 'none'}`))
    })
  })
}

async function waitForCondition(predicate, message, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (predicate()) return
    await new Promise(resolveWait => setTimeout(resolveWait, 25))
  }
  throw new Error(message)
}

async function waitForPreview(child) {
  const deadline = Date.now() + 30_000
  let lastError
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`Vite preview exited before becoming ready (code ${child.exitCode})`)
    }
    try {
      const response = await fetch(baseURL)
      if (response.ok) return
    } catch (error) {
      lastError = error
    }
    await new Promise(resolveWait => setTimeout(resolveWait, 150))
  }
  throw new Error(`Vite preview did not become ready: ${lastError?.message || 'timeout'}`)
}

async function stopPreview(child) {
  if (!child || child.exitCode !== null) return
  if (process.platform === 'win32' && child.pid) {
    await new Promise(resolveStop => {
      const killer = spawn('taskkill', ['/PID', String(child.pid), '/T', '/F'], {
        stdio: 'ignore',
        windowsHide: true,
      })
      killer.once('error', resolveStop)
      killer.once('exit', resolveStop)
    })
  } else {
    child.kill()
  }
  if (child.exitCode !== null) return
  await Promise.race([
    new Promise(resolveExit => child.once('exit', resolveExit)),
    new Promise(resolveWait => setTimeout(resolveWait, 5_000)),
  ])
  if (child.exitCode === null) {
    child.kill('SIGKILL')
    await Promise.race([
      new Promise(resolveExit => child.once('exit', resolveExit)),
      new Promise(resolveWait => setTimeout(resolveWait, 5_000)),
    ])
  }
}

async function assertPreviewStopped() {
  const deadline = Date.now() + 5_000
  while (Date.now() < deadline) {
    try {
      await fetch(baseURL)
    } catch {
      return
    }
    await new Promise(resolveWait => setTimeout(resolveWait, 100))
  }
  throw new Error(`Vite preview still accepts connections after teardown: ${baseURL}`)
}

function timestamp(offsetMs) {
  return new Date(Date.UTC(2026, 7, 11, 4, 0, 0) + offsetMs).toISOString()
}

function stage(name, status, startMs, durationMs, children = []) {
  return {
    span_id: `stage-${name}`,
    name,
    kind: 'stage',
    status,
    started_at: timestamp(startMs),
    finished_at: timestamp(startMs + durationMs),
    duration_ms: durationMs,
    children,
  }
}

function retryNode(spanId, name, startMs, durationMs) {
  return {
    span_id: spanId,
    name,
    kind: 'subspan',
    status: 'failed',
    started_at: timestamp(startMs),
    finished_at: timestamp(startMs + durationMs),
    duration_ms: durationMs,
    error_code: 'FIXTURE_RETRYABLE',
    error_message: `${name} fixture failure`,
    retry_action: { allowed: true, target: name, state: 'failed' },
    children: [],
  }
}

const retryTargets = [
  ['span-summary', 'postprocess.summary'],
  ['span-wiki', 'postprocess.wiki'],
  ['span-graph-0', 'postprocess.graph.chunk[0]'],
  ['span-graph-1', 'postprocess.graph.chunk[1]'],
  ['span-question-0', 'postprocess.question.batch[0]'],
  ['span-question-1', 'postprocess.question.batch[1]'],
  ['span-question-2', 'postprocess.question.batch[2]'],
]

function attemptFourPayload(latestAttempt = 4, parseStatus = 'failed') {
  const summary = retryNode('span-summary', 'postprocess.summary', 410, 90)
  const wiki = retryNode('span-wiki', 'postprocess.wiki', 420, 120)
  const graphChildren = [
    retryNode('span-graph-0', 'postprocess.graph.chunk[0]', 430, 80),
    retryNode('span-graph-1', 'postprocess.graph.chunk[1]', 435, 85),
  ]
  const questionChildren = [
    retryNode('span-question-0', 'postprocess.question.batch[0]', 440, 70),
    retryNode('span-question-1', 'postprocess.question.batch[1]', 445, 75),
    retryNode('span-question-2', 'postprocess.question.batch[2]', 450, 80),
  ]
  const graph = {
    span_id: 'span-graph', name: 'postprocess.graph', kind: 'subspan', status: 'failed',
    started_at: timestamp(425), finished_at: timestamp(530), duration_ms: 105,
    children: graphChildren,
  }
  const question = {
    span_id: 'span-question', name: 'postprocess.question', kind: 'subspan', status: 'failed',
    started_at: timestamp(438), finished_at: timestamp(540), duration_ms: 102,
    children: questionChildren,
  }
  return {
    knowledge_id: 'doc-1',
    attempt: 4,
    latest_attempt: latestAttempt,
    current_attempt: latestAttempt,
    parse_status: parseStatus,
    current_stage: 'postprocess',
    trace: {
      span_id: 'root-4', name: 'knowledge_processing', kind: 'root', status: 'failed',
      started_at: timestamp(0), finished_at: timestamp(550), duration_ms: 550,
      children: [
        stage('docreader', 'done', 0, 100),
        stage('chunking', 'done', 100, 90),
        stage('embedding', 'done', 190, 100),
        stage('multimodal', 'done', 290, 110),
        stage('postprocess', 'failed', 400, 150, [summary, wiki, graph, question]),
      ],
    },
    retry_failed_action: {
      allowed: latestAttempt === 4,
      reason: latestAttempt === 4 ? '' : 'historical_attempt',
      counts: { summary: 1, wiki: 1, graph: 2, question: 3 },
      targets: retryTargets.map(([source_span_id, target_name]) => ({
        source_span_id, target_name, state: 'failed',
      })),
    },
  }
}

function historicalPayload(attempt, latestAttempt) {
  return {
    knowledge_id: 'doc-1', attempt, latest_attempt: latestAttempt, current_attempt: latestAttempt,
    parse_status: 'failed',
    trace: {
      span_id: `root-${attempt}`, name: 'knowledge_processing', kind: 'root', status: 'failed',
      started_at: timestamp(-attempt * 1_000), finished_at: timestamp(-attempt * 1_000 + 200),
      duration_ms: 200,
      children: [
        stage('docreader', 'done', 0, 40),
        stage('chunking', 'done', 40, 40),
        stage('embedding', 'failed', 80, 40),
        stage('multimodal', 'cancelled', 120, 40),
        stage('postprocess', 'cancelled', 160, 40),
      ],
    },
    retry_failed_action: {
      allowed: false,
      reason: 'historical_attempt',
      counts: { summary: 0, wiki: 0, graph: 0, question: 0 },
      targets: [],
    },
  }
}

function attemptFivePayload() {
  const pending = {
    span_id: 'new-summary', name: 'postprocess.summary', kind: 'subspan', status: 'pending',
    started_at: timestamp(1_000), duration_ms: 0, children: [],
  }
  return {
    knowledge_id: 'doc-1', attempt: 5, latest_attempt: 5, current_attempt: 5,
    parse_status: 'completed', current_stage: 'postprocess',
    trace: {
      span_id: 'root-5', name: 'knowledge_processing', kind: 'root', status: 'done',
      started_at: timestamp(900), finished_at: timestamp(1_100), duration_ms: 200,
      children: [
        stage('docreader', 'done', 900, 20),
        stage('chunking', 'done', 920, 20),
        stage('embedding', 'done', 940, 20),
        stage('multimodal', 'done', 960, 20),
        stage('postprocess', 'done', 980, 120, [pending]),
      ],
    },
    retry_failed_action: {
      allowed: false,
      reason: 'no_retryable_targets',
      counts: { summary: 0, wiki: 0, graph: 0, question: 0 },
      targets: [],
    },
  }
}

function profileData(profile) {
  const shared = profile === 'shared-viewer'
  const role = profile === 'admin'
    ? 'admin'
    : profile === 'editor'
      ? 'contributor'
      : 'viewer'
  const kb = {
    id: 'kb-1',
    tenant_id: shared ? 8 : 7,
    creator_id: shared ? 'source-owner' : 'creator-user',
    name: shared ? '共享只读知识库' : '浏览器验证知识库',
    description: 'Production retry smoke fixture',
    type: 'document',
    summary_model_id: 'model-summary',
    embedding_model_id: 'model-embedding',
    my_permission: shared ? 'viewer' : 'owner',
    created_at: timestamp(-10_000),
    updated_at: timestamp(0),
  }
  const doc = {
    id: 'doc-1', knowledge_base_id: 'kb-1', tenant_id: kb.tenant_id,
    file_name: 'retry-fixture.md', title: 'retry-fixture.md', type: 'file', file_type: 'md',
    description: 'Fixture document with retryable processing items.',
    parse_status: 'processing', summary_status: 'processing', error_message: 'fixture postprocess failed',
    source: 'browser-fixture', channel: 'web', file_size: 512,
    created_at: timestamp(-5_000), updated_at: timestamp(0), tags: [], custom_metadata: {},
  }
  const sharedRows = shared ? [{
    share_id: 'share-1', organization_id: 'org-1', org_name: 'Fixture Org', permission: 'viewer',
    source_tenant_id: 8, shared_at: timestamp(-1_000), knowledge_base: kb,
  }] : []
  return { role, kb, doc, sharedRows }
}

function createApiFixture(profile, retryStatus = 202, traceParseStatus = 'failed') {
  const fixture = profileData(profile)
  const state = {
    profile,
    retryStatus,
    accepted: false,
    requests: [],
    retryPosts: [],
    latestSpanReads: 0,
    unknownPaths: new Set(),
  }

  async function json(route, body, status = 200) {
    await route.fulfill({
      status,
      contentType: 'application/json; charset=utf-8',
      body: JSON.stringify(body),
    })
  }

  async function handler(route) {
    const request = route.request()
    const url = new URL(request.url())
    const path = `${url.pathname}${url.search}`
    state.requests.push(`${request.method()} ${path}`)

    if (request.method() === 'POST' && /\/attempts\/4\/(?:spans\/[^/]+\/retry|retry-failed)$/.test(url.pathname)) {
      const post = { path: url.pathname, body: request.postDataJSON() }
      state.retryPosts.push(post)
      await new Promise(resolveWait => setTimeout(resolveWait, 120))
      if (retryStatus !== 202) {
        const messages = {
          403: 'fixture permission denied',
          409: 'fixture retry state changed',
          503: 'fixture retry queue unavailable',
        }
        await json(route, {
          success: false,
          error: { code: `FIXTURE_${retryStatus}`, message: messages[retryStatus] },
        }, retryStatus)
        return
      }
      state.accepted = true
      if (url.pathname.endsWith('/retry-failed')) {
        await json(route, {
          success: true,
          data: {
            knowledge_id: 'doc-1', source_attempt: 4, client_request_id: post.body.client_request_id,
            new_attempt: 5,
            targets: retryTargets.map(([source_span_id, target_name], index) => ({
              source_span_id, target_name, state: 'failed',
              new_span_id: index === 0 ? 'new-summary' : `new-target-${index}`,
              task_id: `fixture-task-${index}`,
            })),
          },
        }, 202)
      } else {
        const encodedSpan = url.pathname.match(/\/spans\/([^/]+)\/retry$/)?.[1] || ''
        const sourceSpanId = decodeURIComponent(encodedSpan)
        const target = retryTargets.find(([spanId]) => spanId === sourceSpanId) || retryTargets[0]
        await json(route, {
          success: true,
          data: {
            source_attempt: 4, source_span_id: sourceSpanId, new_attempt: 5,
            new_span_id: 'new-summary', target_name: target[1], task_id: 'fixture-row-task',
          },
        }, 202)
      }
      return
    }

    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge/doc-1/spans') {
      const attempt = Number(url.searchParams.get('attempt') || 0)
      if (!attempt) state.latestSpanReads += 1
      if (attempt === 5 || (!attempt && state.accepted)) {
        await json(route, { success: true, data: attemptFivePayload() })
      } else if (attempt > 0 && attempt < 4) {
        await json(route, { success: true, data: historicalPayload(attempt, state.accepted ? 5 : 4) })
      } else {
        await json(route, {
          success: true,
          data: attemptFourPayload(state.accepted ? 5 : 4, traceParseStatus),
        })
      }
      return
    }

    if (request.method() === 'GET' && url.pathname === '/api/v1/auth/me') {
      await json(route, {
        success: true,
        data: {
          user: {
            id: 'creator-user', username: 'fixture-user', email: 'fixture@example.test',
            tenant_id: '7', can_access_all_tenants: false, is_system_admin: false,
          },
          tenant: {
            id: '7', name: 'Fixture Tenant', owner_id: 'tenant-owner',
            created_at: timestamp(-10_000), updated_at: timestamp(0),
          },
          memberships: [{ tenant_id: 7, tenant_name: 'Fixture Tenant', role: fixture.role }],
          tenant_required: false,
          capabilities: { can_create_tenant: false, auto_accept_invitation: false },
        },
      })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/system/info') {
      await json(route, { success: true, data: { version: 'browser-smoke' } })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/tenants/kv/retrieval-config') {
      await json(route, { success: true, data: {} })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/shared-agents') {
      await json(route, { success: true, data: [], disabled_own_agent_ids: [] })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/im-channels') {
      await json(route, { success: true, data: [] })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/embed-channels') {
      await json(route, { success: true, data: [] })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/sessions') {
      await json(route, { success: true, data: [], total: 0 })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge/doc-1/preview') {
      await route.fulfill({ status: 200, contentType: 'text/markdown; charset=utf-8', body: '# retry fixture' })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge/batch') {
      await json(route, { success: true, data: [fixture.doc] })
      return
    }

    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge-bases/kb-1/knowledge/folders') {
      await json(route, { success: true, data: { root_document_count: 1, total_document_count: 1, folders: [] } })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge-bases/kb-1/knowledge') {
      await json(route, { success: true, data: [fixture.doc], total: 1 })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge-bases/kb-1/tags') {
      await json(route, { success: true, data: { data: [{ id: 'tag-1', name: 'Fixture', color: '#07c05f' }], total: 1 } })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge-bases/kb-1') {
      await json(route, { success: true, data: fixture.kb })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge-bases') {
      await json(route, { success: true, data: profile === 'shared-viewer' ? [] : [fixture.kb] })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/shared-knowledge-bases') {
      await json(route, { success: true, data: fixture.sharedRows })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/knowledge/doc-1') {
      await json(route, { success: true, data: fixture.doc })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/chunks/doc-1') {
      await json(route, { success: true, data: [], total: 0 })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/organizations') {
      await json(route, { success: true, data: { organizations: [], resource_counts: {} } })
      return
    }
    if (request.method() === 'GET' && url.pathname.includes('/invitations/pending')) {
      await json(route, { success: true, data: { count: 0 } })
      return
    }
    if (request.method() === 'GET' && url.pathname === '/api/v1/agents') {
      await json(route, { success: true, data: [], disabled_own_agent_ids: [] })
      return
    }
    if (request.method() === 'GET' && url.pathname.includes('/models')) {
      await json(route, { success: true, data: [] })
      return
    }
    if (request.method() === 'GET' && url.pathname.includes('/web-search')) {
      await json(route, { success: true, data: [] })
      return
    }
    if (request.method() === 'GET' && (url.pathname.includes('/parser') || url.pathname.includes('/engines'))) {
      await json(route, { success: true, data: [] })
      return
    }

    state.unknownPaths.add(`${request.method()} ${url.pathname}`)
    unmockedApiPaths.add(`${request.method()} ${url.pathname}`)
    await json(route, { success: true, data: [] })
  }

  return { state, handler, fixture }
}

async function seedProfile(page, profile) {
  const { role } = profileData(profile)
  await page.addInitScript(({ membershipRole }) => {
    const user = {
      id: 'creator-user', username: 'fixture-user', email: 'fixture@example.test',
      tenant_id: '7', can_access_all_tenants: false, is_system_admin: false,
    }
    const tenant = {
      id: '7', name: 'Fixture Tenant', owner_id: 'tenant-owner',
      created_at: '2026-08-11T04:00:00.000Z', updated_at: '2026-08-11T04:00:00.000Z',
    }
    localStorage.setItem('weknora_token', 'fixture-token')
    localStorage.setItem('weknora_user', JSON.stringify(user))
    localStorage.setItem('weknora_tenant', JSON.stringify(tenant))
    localStorage.setItem('weknora_selected_tenant_id', '7')
    localStorage.setItem('weknora_selected_tenant_name', 'Fixture Tenant')
    localStorage.setItem('weknora_memberships', JSON.stringify([
      { tenant_id: 7, tenant_name: 'Fixture Tenant', role: membershipRole },
    ]))
    localStorage.setItem('locale', 'zh-CN')
    localStorage.setItem('WeKnora_creator-user_theme', 'light')
    localStorage.setItem('weknora:new-user-guide-done:v1', '1')
    localStorage.setItem('weknora:contextual-guide-kb-detail:v1', '1')
  }, { membershipRole: role })
}

async function openTimeline(context, {
  profile = 'editor',
  retryStatus = 202,
  traceParseStatus = 'failed',
} = {}) {
  const page = await context.newPage()
  const api = createApiFixture(profile, retryStatus, traceParseStatus)
  const pageErrors = []
  page.on('pageerror', error => pageErrors.push(error.message))
  await seedProfile(page, profile)
  await page.route('**/*', async route => {
    const url = new URL(route.request().url())
    if (url.origin === baseURL || url.protocol === 'data:' || url.protocol === 'blob:') {
      await route.continue()
    } else {
      await route.abort('blockedbyclient')
    }
  })
  // Page routes run newest-first. Register the narrower API route last so no
  // `/api/**` request can fall through to Vite's live-backend proxy.
  await page.route('**/api/**', api.handler)

  await page.goto(`${baseURL}/platform/knowledge-bases/kb-1`, { waitUntil: 'networkidle' })
  const card = page.locator('.knowledge-card').filter({ hasNot: page.locator('.knowledge-card-skeleton') }).first()
  await card.waitFor({ state: 'visible' })
  const processingMenuCounts = { reparse: 0, cancel: 0 }
  const menuTrigger = card.locator('.more-wrap')
  if (await menuTrigger.count()) {
    await menuTrigger.click()
    const menu = page.locator('.card-more:visible').last()
    await menu.waitFor({ state: 'visible' })
    processingMenuCounts.reparse = await menu.getByText('重建知识', { exact: true }).count()
    processingMenuCounts.cancel = await menu.getByText('停止解析', { exact: true }).count()
    await menuTrigger.click()
    await menu.waitFor({ state: 'hidden' })
  }
  await card.click()
  await page.locator('.doc-drawer-header-title').waitFor({ state: 'visible' })
  await page.locator('.trace-entry-btn').waitFor({ state: 'visible' })
  await page.locator('.trace-entry-btn').click()
  await page.locator('.kp-secondary-drawer .kp-shell').waitFor({ state: 'visible' })
  assert.deepEqual(pageErrors, [], `page errors on ${profile}: ${pageErrors.join(' | ')}`)
  return { page, processingMenuCounts, ...api }
}

async function visiblePopconfirm(page) {
  const popup = page.locator('.t-popconfirm:visible').last()
  await popup.waitFor({ state: 'visible' })
  return popup
}

async function rapidConfirm(page, trigger, confirmName) {
  await trigger.click()
  const popup = await visiblePopconfirm(page)
  const confirm = popup.getByRole('button', { name: confirmName, exact: true })
  await confirm.waitFor({ state: 'visible' })
  await confirm.evaluate(button => {
    button.click()
    button.click()
  })
}

async function assertPermissionProfiles(context) {
  const shared = await openTimeline(context, { profile: 'shared-viewer' })
  assert.equal(await shared.page.locator('.kp-row-retry').count(), 0, 'shared Viewer must not see row retry')
  assert.equal(await shared.page.getByRole('button', { name: '按需重试失败项', exact: true }).count(), 0,
    'shared Viewer must not see aggregate retry')
  assert.equal(await shared.page.getByRole('button', { name: '重新解析', exact: true }).count(), 0,
    'shared Viewer must not see reparse')
  assert.deepEqual(shared.processingMenuCounts, { reparse: 0, cancel: 0 },
    'shared Viewer must not see document processing mutations')
  await shared.page.close()

  const creatorViewer = await openTimeline(context, { profile: 'creator-viewer' })
  assert.equal(await creatorViewer.page.locator('.kp-row-retry').count(), 0,
    'creator tenant Viewer must not bypass processing role gate')
  assert.equal(await creatorViewer.page.getByRole('button', { name: '按需重试失败项', exact: true }).count(), 0,
    'creator tenant Viewer must not see aggregate retry')
  assert.equal(await creatorViewer.page.getByRole('button', { name: '重新解析', exact: true }).count(), 0,
    'creator tenant Viewer must not see reparse')
  assert.deepEqual(creatorViewer.processingMenuCounts, { reparse: 0, cancel: 0 },
    'creator tenant Viewer must not bypass document processing role gate')
  await creatorViewer.page.close()

  const editor = await openTimeline(context, { profile: 'editor' })
  assert.equal(await editor.page.locator('.kp-row-retry').count(), 7, 'Editor should see every exact retry row')
  assert.equal(await editor.page.getByRole('button', { name: '按需重试失败项', exact: true }).count(), 1,
    'Editor should see aggregate retry')
  assert.equal(await editor.page.getByRole('button', { name: '重新解析', exact: true }).count(), 1,
    'Editor should see reparse')
  assert.deepEqual(editor.processingMenuCounts, { reparse: 1, cancel: 1 },
    'Editor should see reparse and cancel in the document action menu')

  const admin = await openTimeline(context, { profile: 'admin' })
  assert.equal(await admin.page.locator('.kp-row-retry').count(), 7, 'Admin should see every exact retry row')
  assert.equal(await admin.page.getByRole('button', { name: '按需重试失败项', exact: true }).count(), 1,
    'Admin should see aggregate retry')
  assert.equal(await admin.page.getByRole('button', { name: '重新解析', exact: true }).count(), 1,
    'Admin should see reparse')
  assert.deepEqual(admin.processingMenuCounts, { reparse: 1, cancel: 1 },
    'Admin should see reparse and cancel in the document action menu')
  await admin.page.close()
  return editor
}

async function assertEditorInteraction(editor) {
  const { page } = editor
  const counts = page.locator('#kp-retry-summary')
  await counts.waitFor({ state: 'visible' })
  assert.match(await counts.innerText(), /Wiki 1 · 摘要 1 · 图谱 2 · 问题 3/)

  const summaryRow = page.locator('.kp-row[data-span-key="span-summary"]')
  const wikiRow = page.locator('.kp-row[data-span-key="span-wiki"]')
  await summaryRow.focus()
  await page.keyboard.press('Enter')
  await page.locator('.kp-row-active[data-span-key="span-summary"]').waitFor({ state: 'visible' })
  assert.equal(await page.locator('.kp-detail-actions .kp-action-btn').count(), 1, 'selected detail must expose retry')
  await wikiRow.focus()
  await page.keyboard.press('Space')
  await page.locator('.kp-row-active[data-span-key="span-wiki"]').waitFor({ state: 'visible' })

  const refreshButton = page.getByRole('button', { name: /^(立即刷新|自动刷新中)$/ }).first()
  await refreshButton.focus()
  await page.keyboard.press('Tab')
  const focused = await page.evaluate(() => ({
    text: document.activeElement?.textContent?.trim() || '',
    outline: getComputedStyle(document.activeElement).outlineStyle,
    outlineWidth: getComputedStyle(document.activeElement).outlineWidth,
  }))
  assert.match(focused.text, /按需重试失败项/, 'Tab must reach aggregate retry')
  assert.notEqual(focused.outline, 'none', 'keyboard focus must be visibly outlined')
  assert.notEqual(focused.outlineWidth, '0px', 'keyboard focus outline must have width')

  await page.keyboard.press('Enter')
  const keyboardPopup = await visiblePopconfirm(page)
  assert.match(await keyboardPopup.innerText(), /新建一轮局部修复历史/)
  await keyboardPopup.getByRole('button', { name: '取消', exact: true }).click()
  await keyboardPopup.waitFor({ state: 'hidden' })

  const aggregate = page.getByRole('button', { name: '按需重试失败项', exact: true })
  const rowRetry = page.locator('.kp-row-retry').first()
  const detailRetry = page.locator('.kp-detail-actions .kp-action-btn')
  for (const [name, locator] of [['aggregate', aggregate], ['row', rowRetry], ['detail', detailRetry]]) {
    const box = await locator.boundingBox()
    assert(box, `${name} retry must have a bounding box`)
    assert(box.height >= 35.5, `${name} retry height ${box.height} is below 36px`)
    assert(box.width >= 35.5, `${name} retry width ${box.width} is below 36px`)
  }

  await page.evaluate(() => {
    document.documentElement.setAttribute('theme-mode', 'light')
    document.documentElement.style.colorScheme = 'light'
  })
  const lightScheme = await rowRetry.evaluate(element => getComputedStyle(element).colorScheme)
  await page.evaluate(() => {
    document.documentElement.setAttribute('theme-mode', 'dark')
    document.documentElement.style.colorScheme = 'dark'
  })
  const darkScheme = await rowRetry.evaluate(element => getComputedStyle(element).colorScheme)
  assert.match(lightScheme, /light/)
  assert.match(darkScheme, /dark/)

  await rowRetry.click()
  const rowPopup = await visiblePopconfirm(page)
  assert.match(await rowPopup.innerText(), /新建一轮局部修复历史/)
  assert.match(await rowPopup.innerText(), /模型额度/)
  const layers = await page.evaluate(() => {
    const maxZ = element => {
      let current = element
      let max = 0
      while (current) {
        const value = Number.parseInt(getComputedStyle(current).zIndex, 10)
        if (Number.isFinite(value)) max = Math.max(max, value)
        current = current.parentElement
      }
      return max
    }
    const popup = [...document.querySelectorAll('.t-popconfirm')]
      .find(element => element.getBoundingClientRect().width > 0)
    const drawer = document.querySelector('.kp-secondary-drawer')
    return { popup: popup ? maxZ(popup) : 0, drawer: drawer ? maxZ(drawer) : 0 }
  })
  assert(layers.drawer >= 2100, `timeline drawer z-index was ${layers.drawer}`)
  assert(layers.popup >= 2300, `popconfirm z-index was ${layers.popup}`)
  assert(layers.popup > layers.drawer, 'popconfirm must render over timeline drawer')
  await rowPopup.getByRole('button', { name: '取消', exact: true }).click()
  await rowPopup.waitFor({ state: 'hidden' })

  await aggregate.click()
  const aggregatePopup = await visiblePopconfirm(page)
  const confirmation = await aggregatePopup.innerText()
  for (const [, targetName] of retryTargets) assert(confirmation.includes(targetName), `missing target ${targetName}`)
  assert.match(confirmation, /Wiki 1 · 摘要 1 · 图谱 2 · 问题 3/)
  assert.match(confirmation, /模型额度/)
  await aggregatePopup.getByRole('button', { name: '取消', exact: true }).click()
  await aggregatePopup.waitFor({ state: 'hidden' })

  const assets = await page.evaluate(() => performance.getEntriesByType('resource').map(entry => entry.name))
  const assetURL = assets.find(url => /\/assets\/KnowledgeBase-[\w-]+\.js(?:\?|$)/.test(url))
  assert(assetURL, `hashed KnowledgeBase asset not found in ${assets.join('\n')}`)
  const assetContract = await page.evaluate(async resourceURLs => {
    const javascriptURLs = [...new Set(resourceURLs.filter(url => /\.js(?:\?|$)/.test(url)))]
    const needles = ['retry_failed_action', 'retry_action', '/retry-failed', '按需重试失败项', '重试此项']
    const found = Object.fromEntries(needles.map(needle => [needle, '']))
    for (const url of javascriptURLs) {
      const source = await fetch(url).then(response => response.text())
      for (const needle of needles) {
        if (!found[needle] && source.includes(needle)) found[needle] = url
      }
    }
    return found
  }, assets)
  for (const [needle, url] of Object.entries(assetContract)) {
    assert(url, `production JavaScript assets do not contain ${needle}`)
  }

  await page.setViewportSize({ width: 390, height: 844 })
  const mobileAggregateBox = await aggregate.boundingBox()
  const mobileDrawerBox = await page.locator('.kp-secondary-drawer').boundingBox()
  assert(mobileAggregateBox, 'aggregate retry must remain visible at mobile width')
  assert(mobileAggregateBox.height >= 35.5, 'aggregate retry must remain at least 36px tall on mobile')
  assert(mobileAggregateBox.x >= -0.5 && mobileAggregateBox.x + mobileAggregateBox.width <= 390.5,
    `aggregate retry overflows mobile viewport: ${JSON.stringify(mobileAggregateBox)}`)
  assert(mobileDrawerBox, 'timeline drawer must remain visible at mobile width')
  assert(mobileDrawerBox.x >= -0.5 && mobileDrawerBox.x + mobileDrawerBox.width <= 390.5,
    `timeline drawer overflows mobile viewport: ${JSON.stringify(mobileDrawerBox)}`)
  return {
    assetURL,
    assetContract,
    lightScheme,
    darkScheme,
    layers,
    mobile: { aggregate: mobileAggregateBox, drawer: mobileDrawerBox },
  }
}

async function assertSuccess(context, kind) {
  const runCase = await openTimeline(context, { profile: 'editor', retryStatus: 202 })
  const { page, state } = runCase
  if (kind === 'row') {
    await rapidConfirm(page, page.locator('.kp-row-retry').first(), '重试此项')
  } else {
    await rapidConfirm(page, page.getByRole('button', { name: '按需重试失败项', exact: true }), '按需重试失败项')
  }
  await waitForCondition(() => state.retryPosts.length === 1, `${kind} retry POST was not observed`)
  const reparse = page.getByRole('button', { name: '重新解析', exact: true })
  const competingRetry = kind === 'row'
    ? page.getByRole('button', { name: '按需重试失败项', exact: true })
    : page.locator('.kp-row-retry').first()
  assert.equal(await reparse.isDisabled(), true, `${kind} retry must lock whole-document reparse`)
  assert.equal(await competingRetry.isDisabled(), true, `${kind} retry must lock competing retry controls`)
  await page.locator('.kp-attempt-active').filter({ hasText: '#5' }).waitFor({ state: 'visible' })
  await page.locator('.kp-row-active[data-span-key="new-summary"]').waitFor({ state: 'visible' })
  assert.equal(state.retryPosts.length, 1, `${kind} rapid confirm emitted duplicate POSTs`)
  assert.match(state.retryPosts[0].path, kind === 'row' ? /\/spans\/span-summary\/retry$/ : /\/retry-failed$/)
  assert.equal(typeof state.retryPosts[0].body.client_request_id, 'string')
  assert(state.retryPosts[0].body.client_request_id.length > 0)
  const requestEvidence = [...state.requests]
  await page.close()
  return requestEvidence
}

async function assertCancelMutex(context) {
  const runCase = await openTimeline(context, {
    profile: 'editor',
    retryStatus: 202,
    traceParseStatus: 'processing',
  })
  const { page, state } = runCase
  await rapidConfirm(page, page.locator('.kp-row-retry').first(), '重试此项')
  await waitForCondition(() => state.retryPosts.length === 1, 'processing retry POST was not observed')
  assert.equal(await page.getByRole('button', { name: '停止解析', exact: true }).isDisabled(), true,
    'single-item retry must lock whole-document cancel')
  await page.locator('.kp-attempt-active').filter({ hasText: '#5' }).waitFor({ state: 'visible' })
  await page.close()
}

async function assertRetryError(context, status, surface, expectedText, refreshExpected) {
  const runCase = await openTimeline(context, { profile: 'editor', retryStatus: status })
  const { page, state } = runCase
  const baselineReads = state.latestSpanReads
  if (surface === 'row') {
    await rapidConfirm(page, page.locator('.kp-row-retry').first(), '重试此项')
  } else if (surface === 'detail') {
    const row = page.locator('.kp-row[data-span-key="span-summary"]')
    await row.focus()
    await page.keyboard.press('Enter')
    await rapidConfirm(page, page.locator('.kp-detail-actions .kp-action-btn'), '重试此项')
  } else {
    await rapidConfirm(page, page.getByRole('button', { name: '按需重试失败项', exact: true }), '按需重试失败项')
  }
  await page.getByText(expectedText, { exact: true }).waitFor({ state: 'visible' })
  assert.equal(state.retryPosts.length, 1, `${status} ${surface} emitted duplicate POSTs`)
  if (refreshExpected) {
    await waitForCondition(
      () => state.latestSpanReads > baselineReads,
      `${status} should refresh latest spans`,
    )
  } else {
    await page.waitForTimeout(250)
    assert.equal(state.latestSpanReads, baselineReads, `${status} must not trigger state refresh`)
  }
  const requests = [...state.requests]
  await page.close()
  return requests
}

let preview
let browser
let teardown = 'not-started'
try {
  assert(existsSync(viteBin), `frontend Vite executable missing: ${viteBin}`)
  if (process.env.KNOWLEDGE_RETRY_SKIP_BUILD !== '1') {
    await run(process.execPath, [viteBin, 'build'], frontendDir)
  }
  preview = spawn(process.execPath, [viteBin, 'preview', '--host', '127.0.0.1', '--port', String(port), '--strictPort'], {
    cwd: frontendDir,
    stdio: ['ignore', 'pipe', 'pipe'],
    windowsHide: true,
  })
  let previewOutput = ''
  preview.stdout.on('data', chunk => { previewOutput += chunk.toString() })
  preview.stderr.on('data', chunk => { previewOutput += chunk.toString() })
  await waitForPreview(preview)

  const executablePath = chromeCandidates.find(candidate => existsSync(candidate))
  assert(executablePath, `local Chrome was not found; checked ${chromeCandidates.join(', ')}`)
  browser = await chromium.launch({ executablePath, headless: true })
  const context = await browser.newContext({ viewport: { width: 1440, height: 960 }, colorScheme: 'light' })

  const editor = await assertPermissionProfiles(context)
  const visualEvidence = await assertEditorInteraction(editor)
  const editorUnknown = [...editor.state.unknownPaths]
  const editorRequests = [...editor.state.requests]
  await editor.page.close()

  const rowRequests = await assertSuccess(context, 'row')
  const aggregateRequests = await assertSuccess(context, 'aggregate')
  await assertCancelMutex(context)
  const forbiddenRequests = await assertRetryError(
    context, 403, 'row',
    '你没有重试权限，请联系知识库管理员确认 Editor/Admin 权限。', false,
  )
  const conflictRequests = await assertRetryError(
    context, 409, 'detail',
    '状态已变化或已有局部修复轮次；已刷新到最新状态，请检查后再重试。', true,
  )
  const unavailableRequests = await assertRetryError(
    context, 503, 'aggregate',
    '重试任务发布失败；已刷新处理状态，请稍后再次提交。', true,
  )

  await context.close()
  assert.deepEqual(
    [...unmockedApiPaths],
    [],
    `production smoke used unmocked API paths: ${[...unmockedApiPaths].join(', ')}`,
  )
  await browser.close()
  browser = null
  await stopPreview(preview)
  await assertPreviewStopped()
  teardown = preview.exitCode === null ? 'unconfirmed' : 'preview-stopped'

  const retryRequestPaths = [...new Set([
    ...rowRequests, ...aggregateRequests, ...forbiddenRequests, ...conflictRequests, ...unavailableRequests,
  ].filter(entry => entry.startsWith('POST ') || entry.includes('/spans?attempt=5')))]
  console.log(JSON.stringify({
    status: 'GREEN',
    productionRoute: `${baseURL}/platform/knowledge-bases/kb-1`,
    assetURL: visualEvidence.assetURL,
    theme: { light: visualEvidence.lightScheme, dark: visualEvidence.darkScheme },
    layers: visualEvidence.layers,
    mobile: visualEvidence.mobile,
    assetContract: visualEvidence.assetContract,
    retryRequestPaths,
    editorApiPaths: [...new Set(editorRequests)],
    unmockedApiPaths: editorUnknown,
    teardown,
    previewOutput: previewOutput.trim().split(/\r?\n/).slice(-3),
  }, null, 2))
} finally {
  if (browser) await browser.close().catch(() => {})
  if (preview) await stopPreview(preview).catch(() => {})
}
