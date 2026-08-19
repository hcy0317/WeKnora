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
  assert.match(component, /class="form-item engine-lifecycle__mode"/, 'mode field alignment contract is missing')
  assert.match(component, /class="engine-lifecycle__mode-control"/, 'mode control styling hook is missing')
  assert.match(component, /variant="outline"/, 'mode selector must not rely on a partial sliding fill')
  assert.match(component, /\.engine-lifecycle__mode-control[\s\S]*grid-template-columns:\s*repeat\(2, minmax\(0, 1fr\)\)/)
  assert.match(component, /\.engine-lifecycle__mode[\s\S]*\.form-label[\s\S]*text-align:\s*center/)
  assert.match(component, /:deep\(\.t-radio-button\.t-is-checked\)[\s\S]*background(?:-color)?:\s*var\(--td-brand-color\)/)
  assert.match(component, /:deep\(\.t-radio-button:focus-within\)/, 'keyboard focus ring is missing')
  const template = component.slice(0, component.indexOf('<script setup'))
  assert.doesNotMatch(template, /docker|container/i, 'host implementation details leaked into the UI')
})
