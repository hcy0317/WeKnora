import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const botMessage = readFileSync(
  new URL('../views/chat/components/botmsg.vue', import.meta.url),
  'utf8',
)
const agentMessage = readFileSync(
  new URL('../views/chat/components/AgentStreamDisplay.vue', import.meta.url),
  'utf8',
)
const chatView = readFileSync(new URL('../views/chat/index.vue', import.meta.url), 'utf8')
const app = readFileSync(new URL('../App.vue', import.meta.url), 'utf8')

test('production lifecycles invoke the revoking artifact blob disposal APIs', () => {
  for (const component of [botMessage, agentMessage]) {
    assert.match(component, /disposeArtifactBlobURLsForMessage/)
    assert.match(
      component,
      /onBeforeUnmount\([\s\S]*?disposeArtifactBlobURLsForMessage\([\s\S]*?\n\}\);/,
    )
  }

  assert.match(chatView, /disposeArtifactBlobURLsForSession/)
  assert.match(
    chatView,
    /const clearData = \(\) => \{[\s\S]*?disposeArtifactBlobURLsForSession\(session_id\.value\)/,
  )

  assert.match(app, /disposeAllArtifactBlobURLs/)
  assert.match(app, /onUnmounted\([\s\S]*?disposeAllArtifactBlobURLs\(\)/)
})
