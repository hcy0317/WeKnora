import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'

const source = readFileSync(new URL('./KnowledgeBaseEditorModal.vue', import.meta.url), 'utf8')

test('editing a knowledge base closes the editor after a successful save', () => {
  assert.match(source, /emit\('success', kbId\)\s*handleClose\(\)/)
})

test('the first successful create stays open for follow-up configuration', () => {
  const createBranch = source.match(
    /if \(editorMode\.value === 'create'\) \{([\s\S]*?)^\s{4}\} else \{/m
  )?.[1]

  assert.ok(createBranch, 'expected to find the create branch')
  assert.doesNotMatch(createBranch, /handleClose\(\)/)
  assert.match(createBranch, /savedKbId\.value = createdKbId/)
})

test('save button labels distinguish create from save-and-close', () => {
  assert.match(
    source,
    /const saveButtonLabel = computed\(\(\) =>\s*editorMode\.value === 'create'\s*\? t\('knowledgeEditor\.buttons\.create'\)\s*: t\('knowledgeEditor\.buttons\.saveAndClose'\)\s*\)/
  )
})

test('shows a post-create hint after the first successful save', () => {
  assert.match(source, /const isPostCreateSession = computed\(\(\) => !!savedKbId\.value\)/)
  assert.match(source, /settings-footer-note/)
  assert.match(source, /knowledgeEditor\.postCreateHint\.followUpDesc/)
})

test('knowledge graph toggle stays aligned with the indexing strategy', () => {
  assert.match(
    source,
    /formData\.value\.indexingStrategy\.graphEnabled = !!config\.enabled/
  )
})

test('enabled knowledge graph configuration is validated before either save request', () => {
  const validation = source.match(
    /const validateForm = \(\): boolean => \{([\s\S]*?)^\}/m
  )?.[1]

  assert.ok(validation, 'expected to find validateForm')
  assert.match(validation, /graphConfig\?\.enabled/)
  assert.match(validation, /graphConfig\.text\.trim\(\)/)
  assert.match(validation, /graphConfig\.tags\.length/)
  assert.match(validation, /graphConfig\.nodes\.length/)
  assert.match(validation, /graphConfig\.relations\.length/)
  assert.match(validation, /currentSection\.value = 'graph'/)
  assert.match(validation, /graphSettings\.configIncomplete/)

  const submit = source.match(
    /const handleSubmit = async \(\) => \{([\s\S]*?)^const doSubmit/m
  )?.[1]
  assert.ok(submit, 'expected to find handleSubmit')
  assert.ok(
    submit.indexOf('validateForm()') < submit.indexOf('doSubmit()'),
    'validation must run before submission starts'
  )
})
