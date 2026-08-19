<template>
  <div class="engine-lifecycle">
    <div class="engine-lifecycle__heading">
      <div>
        <h4 class="setting-drawer__section-title">{{ t('settings.engineLifecycle.title') }}</h4>
        <p class="engine-lifecycle__description">{{ t('settings.engineLifecycle.description') }}</p>
      </div>
      <t-tag v-if="status" :theme="statusTheme" variant="light" size="small">
        {{ status.state }}
      </t-tag>
    </div>

    <t-alert
      v-if="!authStore.isSystemAdmin"
      theme="info"
      :message="t('settings.engineLifecycle.adminOnly')"
    />

    <template v-else>
      <t-alert
        v-if="!lifecycleStore.controllerOnline && !lifecycleStore.loading"
        theme="warning"
        :message="t('settings.engineLifecycle.controllerOffline')"
      />
      <t-alert
        v-if="lifecycleStore.config?.observe_only"
        theme="warning"
        :message="t('settings.engineLifecycle.observeOnly')"
      />
      <t-alert
        v-if="conflictDetected"
        theme="warning"
        :message="conflictMessage"
      />

      <div v-if="status" class="engine-lifecycle__metrics">
        <span>{{ t('settings.engineLifecycle.active') }}: {{ status.active ?? 0 }}</span>
        <span>{{ t('settings.engineLifecycle.pending') }}: {{ status.pending ?? 0 }}</span>
        <span v-if="showGPUAdmission">
          {{ t('settings.engineLifecycle.gpuAdmission') }}:
          {{ t(status.gpu_admission_allowed
            ? 'settings.engineLifecycle.admissionOpen'
            : 'settings.engineLifecycle.admissionClosed') }}
        </span>
        <span>{{ t('settings.engineLifecycle.revision') }}: {{ lifecycleStore.config?.revision }}</span>
      </div>

      <div v-if="lifecycleStore.loading && !draftInitialized" class="engine-lifecycle__loading">
        {{ t('common.loading') }}
      </div>

      <div v-else-if="draftInitialized" class="engine-lifecycle__form">
        <div class="form-item engine-lifecycle__mode">
          <label class="form-label">{{ t('settings.engineLifecycle.mode') }}</label>
          <t-radio-group v-model="mode" class="engine-lifecycle__mode-control" variant="outline">
            <t-radio-button value="on_demand">
              {{ t('settings.engineLifecycle.onDemand') }}
            </t-radio-button>
            <t-radio-button value="always_on">
              {{ t('settings.engineLifecycle.alwaysOn') }}
            </t-radio-button>
          </t-radio-group>
          <p class="form-desc">
            {{ mode === 'always_on'
              ? t('settings.engineLifecycle.alwaysOnHint')
              : t('settings.engineLifecycle.onDemandHint') }}
          </p>
        </div>

        <div v-if="mode === 'on_demand'" class="form-item engine-lifecycle__override">
          <t-checkbox v-model="overrideIdle">
            {{ t('settings.engineLifecycle.idleOverride') }}
          </t-checkbox>
          <t-input-number
            v-if="overrideIdle"
            v-model="idleMinutes"
            :min="1"
            :step="1"
            theme="column"
          />
          <p class="form-desc">
            {{ t('settings.engineLifecycle.defaultIdle', {
              minutes: lifecycleStore.config?.defaults.idle_minutes ?? 10,
            }) }}
          </p>
        </div>

        <div class="form-item engine-lifecycle__override">
          <t-checkbox v-model="overrideStartup">
            {{ t('settings.engineLifecycle.startupOverride') }}
          </t-checkbox>
          <t-input-number
            v-if="overrideStartup"
            v-model="startupTimeoutSeconds"
            :min="1"
            :step="1"
            theme="column"
          />
          <p class="form-desc">
            {{ t('settings.engineLifecycle.defaultStartup', {
              seconds: lifecycleStore.config?.defaults.startup_timeout_seconds ?? 120,
            }) }}
          </p>
        </div>

        <div class="engine-lifecycle__actions">
          <t-button
            theme="primary"
            size="small"
            :loading="lifecycleStore.saving"
            :disabled="!lifecycleStore.controllerOnline || !draftValid || conflictNeedsRefresh"
            @click="save"
          >
            {{ t('settings.engineLifecycle.apply') }}
          </t-button>
          <t-button
            variant="outline"
            size="small"
            :loading="lifecycleStore.loading"
            @click="refresh"
          >
            {{ t('settings.engineLifecycle.refresh') }}
          </t-button>
        </div>
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { MessagePlugin } from 'tdesign-vue-next'
import { useI18n } from 'vue-i18n'

import { useAuthStore } from '@/stores/auth'
import { useEngineLifecycleStore } from '@/stores/engineLifecycle'
import type {
  EngineLifecycleGroup,
  EngineLifecycleMode,
} from '@/stores/engineLifecyclePolicy'
import { shouldShowGPUAdmission } from '@/stores/engineLifecyclePolicy'

const props = defineProps<{ group: EngineLifecycleGroup }>()
const { t } = useI18n()
const authStore = useAuthStore()
const lifecycleStore = useEngineLifecycleStore()

const mode = ref<EngineLifecycleMode>('on_demand')
const overrideIdle = ref(false)
const idleMinutes = ref(10)
const overrideStartup = ref(false)
const startupTimeoutSeconds = ref(120)
const draftInitialized = ref(false)
const conflictDetected = ref(false)
const conflictRefreshed = ref(false)

const policy = computed(() => lifecycleStore.config?.groups[props.group])
const status = computed(() => policy.value?.status)
const showGPUAdmission = computed(() =>
  shouldShowGPUAdmission(props.group, status.value?.gpu_admission_allowed),
)
const statusTheme = computed(() => {
  if (status.value?.state === 'ready' || status.value?.state === 'busy') return 'success'
  if (status.value?.state === 'failed') return 'danger'
  if (status.value?.state === 'starting' || status.value?.state === 'draining') return 'warning'
  return 'default'
})
const draftValid = computed(() => (
  (!overrideIdle.value || Number.isInteger(idleMinutes.value) && idleMinutes.value >= 1)
  && (!overrideStartup.value
    || Number.isInteger(startupTimeoutSeconds.value) && startupTimeoutSeconds.value >= 1)
))
const conflictNeedsRefresh = computed(() => conflictDetected.value && !conflictRefreshed.value)
const conflictMessage = computed(() => {
  if (conflictRefreshed.value) return t('settings.engineLifecycle.conflictRefreshed')
  return t('settings.engineLifecycle.conflict', {
    revision: lifecycleStore.conflictRevision ?? '?',
  })
})

function syncDraft(): void {
  const current = policy.value
  const defaults = lifecycleStore.config?.defaults
  if (!current || !defaults) return
  mode.value = current.mode
  overrideIdle.value = current.idle_minutes != null
  idleMinutes.value = current.idle_minutes ?? defaults.idle_minutes
  overrideStartup.value = current.startup_timeout_seconds != null
  startupTimeoutSeconds.value = current.startup_timeout_seconds ?? defaults.startup_timeout_seconds
  draftInitialized.value = true
}

async function loadInitial(): Promise<void> {
  const loaded = await lifecycleStore.load()
  if (loaded && !draftInitialized.value) syncDraft()
}

async function refresh(): Promise<void> {
  const preserveDraft = conflictDetected.value
  const loaded = await lifecycleStore.refresh()
  if (!loaded) return
  if (preserveDraft) {
    conflictRefreshed.value = true
  } else {
    syncDraft()
  }
}

async function save(): Promise<void> {
  if (!draftValid.value) return
  const result = await lifecycleStore.saveGroup(props.group, {
    mode: mode.value,
    ...(overrideIdle.value
      ? { idle_minutes: idleMinutes.value }
      : {}),
    ...(overrideStartup.value
      ? { startup_timeout_seconds: startupTimeoutSeconds.value }
      : {}),
  })
  if (result.ok) {
    conflictDetected.value = false
    conflictRefreshed.value = false
    syncDraft()
    MessagePlugin.success(t('settings.engineLifecycle.saved'))
    return
  }
  if (result.kind === 'conflict') {
    conflictDetected.value = true
    conflictRefreshed.value = false
    return
  }
  MessagePlugin.error(t('settings.engineLifecycle.saveFailed'))
}

watch(
  () => authStore.isSystemAdmin,
  (isAdmin) => {
    if (isAdmin) void loadInitial()
  },
  { immediate: true },
)
</script>

<style scoped lang="less">
.engine-lifecycle {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid var(--td-component-border);
}

.engine-lifecycle__heading,
.engine-lifecycle__actions,
.engine-lifecycle__metrics {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.engine-lifecycle__heading .setting-drawer__section-title {
  margin: 0;
}

.engine-lifecycle__description,
.engine-lifecycle__loading {
  margin: 4px 0 0;
  color: var(--td-text-color-secondary);
  font-size: 12px;
}

.engine-lifecycle__metrics {
  justify-content: flex-start;
  flex-wrap: wrap;
  color: var(--td-text-color-secondary);
  font-size: 12px;
}

.engine-lifecycle__metrics span + span::before {
  content: '·';
  margin-right: 8px;
}

.engine-lifecycle__form,
.engine-lifecycle__override {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.engine-lifecycle__mode {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 8px;
}

.engine-lifecycle__mode > .form-label {
  width: 100%;
  margin: 0;
  text-align: center;
}

.engine-lifecycle__mode-control {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  width: 100%;
  overflow: hidden;
  border: 1px solid var(--td-component-stroke);
  border-radius: var(--td-radius-default);
  background: var(--td-bg-color-container);
}

.engine-lifecycle__mode-control :deep(.t-radio-button) {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  min-width: 0;
  min-height: 36px;
  padding: 0 12px;
  border: 0 !important;
  border-radius: 0;
  background: var(--td-bg-color-container);
  color: var(--td-text-color-primary);
  box-sizing: border-box;
  transition: background-color 150ms ease, color 150ms ease;
}

.engine-lifecycle__mode-control :deep(.t-radio-button + .t-radio-button) {
  border-left: 1px solid var(--td-component-stroke) !important;
}

.engine-lifecycle__mode-control :deep(.t-radio-button:hover:not(.t-is-disabled):not(.t-is-checked)) {
  background: var(--td-bg-color-container-hover);
  color: var(--td-brand-color);
}

.engine-lifecycle__mode-control :deep(.t-radio-button.t-is-checked) {
  background: var(--td-brand-color);
  color: var(--td-text-color-anti);
}

.engine-lifecycle__mode-control :deep(.t-radio-button.t-is-checked > span),
.engine-lifecycle__mode-control :deep(.t-radio-button.t-is-checked .t-radio-button__label) {
  background: transparent;
  color: inherit;
}

.engine-lifecycle__mode-control :deep(.t-radio-button.t-is-checked:hover:not(.t-is-disabled)) {
  background: var(--td-brand-color-active);
  color: var(--td-text-color-anti);
}

.engine-lifecycle__mode-control :deep(.t-radio-button:focus-within) {
  z-index: 1;
  outline: 2px solid var(--td-brand-color-focus);
  outline-offset: -2px;
}

.engine-lifecycle__actions {
  justify-content: flex-start;
  margin-top: 4px;
}
</style>
