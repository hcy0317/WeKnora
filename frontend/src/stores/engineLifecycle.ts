import { defineStore } from 'pinia'
import { ref } from 'vue'

import { getEngineLifecycle, updateEngineLifecycle } from '@/api/system'
import {
  buildEngineLifecycleUpdate,
  classifyEngineLifecycleError,
  mergeEngineLifecycleConfig,
  type EngineLifecycleConfig,
  type EngineLifecycleGroup,
  type EngineLifecycleGroupPolicy,
} from '@/stores/engineLifecyclePolicy'

export interface EngineLifecycleSaveResult {
  ok: boolean
  kind?: 'conflict' | 'offline' | 'other'
  revision?: number
}

export const useEngineLifecycleStore = defineStore('engineLifecycle', () => {
  const config = ref<EngineLifecycleConfig | null>(null)
  const loading = ref(false)
  const saving = ref(false)
  const controllerOnline = ref(false)
  const error = ref('')
  const conflictRevision = ref<number | null>(null)
  let loadPromise: Promise<boolean> | null = null

  async function load(force = false): Promise<boolean> {
    if (!force && config.value) return true
    if (loadPromise) return loadPromise

    loading.value = true
    loadPromise = (async () => {
      try {
        const next = await getEngineLifecycle()
        config.value = next
        controllerOnline.value = next.controller_online
        error.value = ''
        conflictRevision.value = null
        return true
      } catch (cause: unknown) {
        const state = classifyEngineLifecycleError(cause)
        if (state.kind === 'offline') controllerOnline.value = false
        error.value = typeof cause === 'object' && cause !== null && 'message' in cause
          ? String((cause as { message?: unknown }).message ?? '')
          : ''
        return false
      } finally {
        loading.value = false
        loadPromise = null
      }
    })()
    return loadPromise
  }

  async function refresh(): Promise<boolean> {
    return load(true)
  }

  async function saveGroup(
    group: EngineLifecycleGroup,
    draft: Omit<EngineLifecycleGroupPolicy, 'status'>,
  ): Promise<EngineLifecycleSaveResult> {
    const current = config.value
    if (!current || !controllerOnline.value) return { ok: false, kind: 'offline' }

    saving.value = true
    error.value = ''
    conflictRevision.value = null
    try {
      const payload = buildEngineLifecycleUpdate(current, group, draft)
      const updated = await updateEngineLifecycle(current.revision, payload)
      config.value = mergeEngineLifecycleConfig(current, updated)
      controllerOnline.value = updated.controller_online
      return { ok: true }
    } catch (cause: unknown) {
      const state = classifyEngineLifecycleError(cause)
      if (state.kind === 'conflict') {
        conflictRevision.value = state.revision ?? null
        return { ok: false, kind: 'conflict', revision: state.revision }
      }
      if (state.kind === 'offline') controllerOnline.value = false
      error.value = typeof cause === 'object' && cause !== null && 'message' in cause
        ? String((cause as { message?: unknown }).message ?? '')
        : ''
      return { ok: false, kind: state.kind }
    } finally {
      saving.value = false
    }
  }

  return {
    config,
    loading,
    saving,
    controllerOnline,
    error,
    conflictRevision,
    load,
    refresh,
    saveGroup,
  }
})
