import assert from 'node:assert/strict'
import { existsSync, readFileSync } from 'node:fs'
import test from 'node:test'

const parserPath = new URL('../../views/settings/ParserEngineSettings.vue', import.meta.url)
const modelPath = new URL('../ModelEditorDialog.vue', import.meta.url)
const componentPath = new URL('./EngineLifecycleSettings.vue', import.meta.url)

test('managed engine settings expose one shared lifecycle editor at the intended entry points', () => {
  assert.equal(existsSync(componentPath), true, 'missing shared lifecycle settings component')

  const parser = readFileSync(parserPath, 'utf8')
  const model = readFileSync(modelPath, 'utf8')
  const component = readFileSync(componentPath, 'utf8')

  assert.match(parser, /<EngineLifecycleSettings\s+group="paddleocr"/, 'Paddle self-hosted drawer is not wired')
  assert.match(model, /managedEngineGroup\(activeModelType\.value,\s*formData\.value\.baseUrl\)/, 'model endpoint classifier is not wired')
  assert.match(model, /<EngineLifecycleSettings\s+:group="lifecycleGroup"/, 'model drawer is not wired')
  assert.match(component, /authStore\.isSystemAdmin/, 'SystemAdmin UI gate is missing')
  assert.match(component, /:min="1"/, 'one-minute lower bound is missing')
  assert.doesNotMatch(component, /docker|container/i, 'host implementation details leaked into the UI')
})
