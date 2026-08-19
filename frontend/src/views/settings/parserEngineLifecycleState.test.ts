import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const api = readFileSync(new URL('../../api/system/index.ts', import.meta.url), 'utf8')
const component = readFileSync(new URL('./ParserEngineSettings.vue', import.meta.url), 'utf8')
const localeFiles = ['en-US', 'zh-CN', 'ko-KR', 'ru-RU'].map(name =>
  readFileSync(new URL(`../../i18n/locales/${name}.ts`, import.meta.url), 'utf8'),
)

test('parser engine state contract preserves managed standby as available', () => {
  assert.match(
    api,
    /State\?:\s*'ready'\s*\|\s*'standby'\s*\|\s*'starting'\s*\|\s*'unavailable'\s*\|\s*'unknown'/,
    'ParserEngineInfo is missing the lifecycle state union',
  )
  assert.match(component, /engine\.State === 'standby'/, 'standby is not rendered explicitly')
  assert.match(component, /settings\.parser\.standby/, 'standby copy is not wired')
  for (const locale of localeFiles) {
    assert.match(locale, /standby:\s*['"][^'"]+['"]/, 'a locale is missing standby copy')
    assert.match(locale, /starting:\s*['"][^'"]+['"]/, 'a locale is missing starting copy')
    assert.match(locale, /unknown:\s*['"][^'"]+['"]/, 'a locale is missing unknown copy')
  }
})
