import assert from 'node:assert/strict'
import test from 'node:test'

import {
  ArtifactBlobRequestCoordinator,
  ArtifactBlobURLCache,
  isArtifactRefHref,
  normalizeSandboxArtifactRefs,
  renderArtifactReference,
  resolveArtifactRef,
  type ArtifactRefMeta,
} from './sandboxArtifactRefs.ts'

const labels = { previewHint: '点击预览', missingHint: '文件不可用' }

// 22 位句柄，与后端 types.ResourceHandleLength 一致。
const handleFor = (i: number) => `art${i}`.padEnd(22, 'x')
const refFor = (i: number) => `resource://${handleFor(i)}`

const artifacts: ArtifactRefMeta[] = [
  { index: 0, handle: refFor(0), file_name: '市场画像评分_e7edba.html', file_type: 'text/html' },
  { index: 1, handle: refFor(1), file_name: 'trend.png', file_type: 'image/png' },
  { index: 2, handle: refFor(2), file_name: 'diagram.svg', file_type: 'image/svg+xml' },
  { index: 3, handle: refFor(3), file_name: '腾讯控股(00700) 成交量_838ccc.html', file_type: 'text/html' },
]

// 知识库检索图：形式与产物完全相同，但不属于这条消息。
const foreignRef = `resource://${'z'.repeat(22)}`

test('isArtifactRefHref matches handle and sandbox forms only', () => {
  assert.equal(isArtifactRefHref(refFor(0)), true)
  assert.equal(isArtifactRefHref('sandbox:trend.png'), true)
  assert.equal(isArtifactRefHref('sandbox://trend.png'), true)
  assert.equal(isArtifactRefHref('https://example.com/trend.png'), false)
  // 长度不对的不是句柄。
  assert.equal(isArtifactRefHref('resource://tenant/1/trend.png'), false)
  assert.equal(isArtifactRefHref('trend.png'), false)
  assert.equal(isArtifactRefHref(''), false)
})

test('resolveArtifactRef resolves both the handle and the name form', () => {
  assert.equal(resolveArtifactRef(refFor(1), artifacts)?.file_name, 'trend.png')
  assert.equal(resolveArtifactRef('sandbox:trend.png', artifacts)?.index, 1)
  assert.equal(
    resolveArtifactRef('sandbox:/workspace/output/trend.png', artifacts)?.index,
    1,
  )
  assert.equal(
    resolveArtifactRef('sandbox:%E5%B8%82%E5%9C%BA%E7%94%BB%E5%83%8F%E8%AF%84%E5%88%86_e7edba.html', artifacts)?.index,
    0,
  )
  assert.equal(resolveArtifactRef(foreignRef, artifacts), null)
  assert.equal(resolveArtifactRef('sandbox:missing.csv', artifacts), null)
})

test('resolveArtifactRef accepts the url field carried by persisted messages', () => {
  const persisted: ArtifactRefMeta[] = [{ index: 0, url: refFor(7), file_name: 'a.csv' }]
  assert.equal(resolveArtifactRef(refFor(7), persisted)?.file_name, 'a.csv')
})

test('renderArtifactReference leaves ordinary images to the default renderer', () => {
  assert.equal(
    renderArtifactReference({ href: 'https://example.com/a.png', artifacts, labels }),
    null,
  )
  assert.equal(
    renderArtifactReference({ href: 'resource://tenant/1/a.png', artifacts, labels }),
    null,
  )
})

test('a handle from outside this message falls back to protected-image rendering', () => {
  // 知识库检索图与产物同形。此时必须交回默认渲染，而不是显示「文件不可用」。
  assert.equal(
    renderArtifactReference({ href: foreignRef, artifacts, labels }),
    null,
  )
  assert.equal(
    renderArtifactReference({ href: foreignRef, artifacts: [], labels, streaming: true }),
    null,
  )
})

test('renderArtifactReference renders image artifacts inline', () => {
  const html = renderArtifactReference({
    href: refFor(1),
    alt: '走势',
    artifacts,
    labels,
  })
  assert.ok(html?.includes('class="markdown-image artifact-ref-image"'))
  assert.ok(html?.includes('data-artifact-index="1"'))
  assert.ok(html?.includes('data-img-loading="1"'))
  assert.ok(html?.includes('alt="走势"'))
})

test('renderArtifactReference renders non-image artifacts as a clickable card', () => {
  const html = renderArtifactReference({
    href: refFor(0),
    alt: '市场画像评分',
    artifacts,
    labels,
  })
  assert.ok(html?.includes('class="artifact-ref-card"'))
  assert.ok(html?.includes('data-artifact-index="0"'))
  assert.ok(html?.includes('role="button"'))
  assert.ok(html?.includes('市场画像评分_e7edba.html'))
  assert.ok(html?.includes('点击预览'))
  assert.ok(!html?.includes('<img'))
})

test('svg artifacts go through the sandboxed preview rather than inline rendering', () => {
  const html = renderArtifactReference({ href: refFor(2), artifacts, labels })
  assert.ok(html?.includes('artifact-ref-card'))
  assert.ok(!html?.includes('artifact-ref-image'))
})

test('while streaming an unresolved reference shows the image skeleton', () => {
  // Artifacts are only collected once the turn ends, so mid-stream every
  // reference is unresolved; a card there would flash a half-written name.
  const html = renderArtifactReference({
    href: 'sandbox:市场画像评分_e7edba.html',
    artifacts: [],
    labels,
    streaming: true,
  })
  assert.ok(html?.includes('streaming-image-loading'))
  assert.ok(!html?.includes('artifact-ref-card'))
})

test('after the turn ends an unresolved reference says so instead of hanging', () => {
  const html = renderArtifactReference({
    href: 'sandbox:市场画像评分_e7edba.html',
    alt: '市场画像评分',
    artifacts: [],
    labels,
  })
  assert.ok(html?.includes('artifact-ref-card--pending'))
  assert.ok(html?.includes('文件不可用'))
  assert.ok(!html?.includes('data-artifact-index'))
  assert.ok(!html?.includes('role="button"'))
})

test('file names with spaces and parentheses resolve end to end', () => {
  const raw = '![成交量](sandbox:腾讯控股(00700) 成交量_838ccc.html)'
  const normalized = normalizeSandboxArtifactRefs(raw)

  // marked would split the raw destination at the first space; after
  // normalization it is a single token.
  const href = normalized.slice(normalized.indexOf('](') + 2, normalized.lastIndexOf(')'))
  assert.ok(!href.includes(' '))
  assert.equal(resolveArtifactRef(href, artifacts)?.index, 3)

  const html = renderArtifactReference({ href, alt: '成交量', artifacts, labels })
  assert.ok(html?.includes('腾讯控股(00700) 成交量_838ccc.html'))
  assert.ok(html?.includes('data-artifact-index="3"'))
})

test('normalizeSandboxArtifactRefs leaves everything else alone', () => {
  const untouched = [
    '![远程](https://example.com/a(b).png)',
    '![资源](resource://tenant/1/a.png)',
    '腾讯控股(00700) 的成交量见下图。',
    '写成 `![图](sandbox:a b.html)` 即可',
    '```\n![图](sandbox:a b.html)\n```',
  ]
  for (const markdown of untouched) {
    assert.equal(normalizeSandboxArtifactRefs(markdown), markdown)
  }
})

test('normalizeSandboxArtifactRefs preserves a title and an unterminated tail', () => {
  assert.equal(
    normalizeSandboxArtifactRefs('![图](sandbox:a b.html "说明")'),
    '![图](sandbox:a%20b.html "说明")',
  )
  // Mid-stream the closing paren has not arrived yet; leave the tail for the
  // streaming placeholder guard rather than guessing where it ends.
  assert.equal(
    normalizeSandboxArtifactRefs('![图](sandbox:腾讯控股(00700'),
    '![图](sandbox:腾讯控股(00700',
  )
})

test('artifact names are HTML-escaped', () => {
  const html = renderArtifactReference({
    href: refFor(0),
    artifacts: [{ index: 0, handle: refFor(0), file_name: '<img src=x onerror=alert(1)>.csv' }],
    labels,
  })
  assert.ok(!html?.includes('<img src=x'))
  assert.ok(html?.includes('&lt;img'))
})

test('artifact blob cache is LRU-bounded and revokes evicted URLs', () => {
  const revoked: string[] = []
  const cache = new ArtifactBlobURLCache(2, (url) => revoked.push(url))
  const ctx = { sessionId: 'session-a', messageId: 'message-a' }

  cache.set(ctx, 0, 'blob:first')
  cache.set(ctx, 1, 'blob:second')
  assert.equal(cache.get(ctx, 0), 'blob:first')
  cache.set(ctx, 2, 'blob:third')

  assert.equal(cache.size, 2)
  assert.equal(cache.get(ctx, 1), undefined)
  assert.deepEqual(revoked, ['blob:second'])
})

test('artifact blob cache disposes one message or a whole session', () => {
  const revoked: string[] = []
  const cache = new ArtifactBlobURLCache(10, (url) => revoked.push(url))
  const firstMessage = { sessionId: 'session-a', messageId: 'message-a' }
  const secondMessage = { sessionId: 'session-a', messageId: 'message-b' }
  const otherSession = { sessionId: 'session-b', messageId: 'message-a' }

  cache.set(firstMessage, 0, 'blob:first-message')
  cache.set(secondMessage, 0, 'blob:second-message')
  cache.set(otherSession, 0, 'blob:other-session')

  cache.disposeMessage(firstMessage)
  assert.equal(cache.get(firstMessage, 0), undefined)
  assert.equal(cache.get(secondMessage, 0), 'blob:second-message')

  cache.disposeSession('session-a')
  assert.equal(cache.get(secondMessage, 0), undefined)
  assert.equal(cache.get(otherSession, 0), 'blob:other-session')
  assert.deepEqual(revoked, ['blob:first-message', 'blob:second-message'])
})

test('artifact blob request disposal detaches only the obsolete generation', async () => {
  const coordinator = new ArtifactBlobRequestCoordinator<string>()
  const accepted: string[] = []
  const discarded: string[] = []
  let resolveFirst!: (value: string) => void
  let resolveSecond!: (value: string) => void
  const firstValue = new Promise<string>((resolve) => { resolveFirst = resolve })
  const secondValue = new Promise<string>((resolve) => { resolveSecond = resolve })
  const key = 'session-a\u0000message-a\u00000'

  const first = coordinator.load(
    key,
    () => firstValue,
    (value) => accepted.push(value),
    (value) => discarded.push(value),
  )
  coordinator.detachMatching((candidate) => candidate.startsWith('session-a\u0000message-a\u0000'))
  const second = coordinator.load(
    key,
    () => secondValue,
    (value) => accepted.push(value),
    (value) => discarded.push(value),
  )

  resolveFirst('blob:obsolete')
  assert.equal(await first, null)
  assert.deepEqual(discarded, ['blob:obsolete'])

  // The obsolete request's finally block must not remove the remounted
  // generation: a concurrent consumer still shares the second promise.
  const sharedSecond = coordinator.load(
    key,
    async () => 'blob:unexpected-third',
    (value) => accepted.push(value),
    (value) => discarded.push(value),
  )
  assert.equal(sharedSecond, second)

  resolveSecond('blob:current')
  assert.equal(await second, 'blob:current')
  assert.deepEqual(accepted, ['blob:current'])
  assert.deepEqual(discarded, ['blob:obsolete'])
})
