import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const editor = readFileSync(new URL('./ModelEditorDialog.vue', import.meta.url), 'utf8')
const settings = readFileSync(new URL('../views/settings/ModelSettings.vue', import.meta.url), 'utf8')

test('shows thinking control only for chat and reasoning effort for chat plus VLM', () => {
  assert.match(editor, /<div v-if="showThinkingControlField"[\s\S]*?thinkingControlLabel/)
  assert.match(editor, /<div v-if="showReasoningEffortField"[\s\S]*?reasoningEffortLabel/)
  assert.match(editor, /activeModelType\.value === 'chat' && formData\.value\.source === 'remote'/)
  assert.match(editor, /activeModelType\.value === 'chat' \|\| activeModelType\.value === 'vllm'/)
})

test('persists reasoning effort for VLM without persisting thinking control', () => {
  assert.match(settings, /saveType === 'chat'[\s\S]*?extraConfig\.thinking_control/)
  assert.match(settings, /saveType === 'chat' \|\| saveType === 'vllm'[\s\S]*?extraConfig\.reasoning_effort/)
})

test('VLM connection test declares its real model type', () => {
  assert.match(editor, /case 'vllm':[\s\S]*?modelType: 'vllm'/)
})
