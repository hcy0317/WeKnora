export type EngineLifecycleGroup = 'paddleocr' | 'asr' | 'reranker'
export type EngineLifecycleMode = 'on_demand' | 'always_on'

export interface EngineLifecycleSnapshot {
  group: EngineLifecycleGroup
  state: string
  epoch?: number
  pending?: number
  active?: number
  suspect?: number
  shadow?: number
  gpu_admission_allowed?: boolean
}

export interface EngineLifecycleDefaults {
  idle_minutes: number
  startup_timeout_seconds: number
  failure_cooldown_minutes: number
}

export interface EngineLifecycleGroupPolicy {
  mode: EngineLifecycleMode
  idle_minutes?: number
  startup_timeout_seconds?: number
  status?: EngineLifecycleSnapshot
}

export interface EngineLifecycleConfig {
  controller_online: boolean
  observe_only: boolean
  revision: number
  defaults: EngineLifecycleDefaults
  groups: Record<EngineLifecycleGroup, EngineLifecycleGroupPolicy>
}

export interface EngineLifecycleUpdatePayload {
  defaults: Pick<EngineLifecycleDefaults, 'idle_minutes' | 'startup_timeout_seconds'>
  groups: Record<EngineLifecycleGroup, Omit<EngineLifecycleGroupPolicy, 'status'>>
}

export type EngineLifecycleErrorState =
  | { kind: 'conflict'; revision?: number }
  | { kind: 'offline' }
  | { kind: 'other' }

export function shouldShowGPUAdmission(
  group: EngineLifecycleGroup,
  allowed: boolean | undefined,
): boolean {
  return group !== 'asr' && allowed != null
}

export function classifyEngineLifecycleError(error: unknown): EngineLifecycleErrorState {
  if (typeof error !== 'object' || error === null) return { kind: 'other' }
  const candidate = error as { status?: unknown; revision?: unknown }
  if (candidate.status === 409) {
    return {
      kind: 'conflict',
      ...(typeof candidate.revision === 'number' ? { revision: candidate.revision } : {}),
    }
  }
  if (candidate.status === 503) return { kind: 'offline' }
  return { kind: 'other' }
}

const managedEndpoints: Record<EngineLifecycleGroup, ReadonlySet<string>> = {
  paddleocr: new Set([
    'http://paddleocr-vl:8080',
    'http://engine-gateway:18084/paddleocr',
  ]),
  asr: new Set([
    'http://speaches:8000/v1',
    'http://engine-gateway:18084/asr/v1',
  ]),
  reranker: new Set([
    'http://accelerator-router:18083/v1',
    'http://qwen-reranker-gpu:8000',
    'http://qwen-reranker-gpu:8000/v1',
    'http://qwen-reranker:8000',
    'http://qwen-reranker:8000/v1',
    'http://engine-gateway:18084/reranker',
  ]),
}

export function managedEngineGroup(modelType: string, endpoint?: string): EngineLifecycleGroup | null {
  const normalized = (endpoint ?? '').trim().replace(/\/+$/, '')
  if (modelType === 'asr' && managedEndpoints.asr.has(normalized)) return 'asr'
  if (modelType === 'rerank' && managedEndpoints.reranker.has(normalized)) return 'reranker'
  if (modelType === 'paddleocr' && managedEndpoints.paddleocr.has(normalized)) return 'paddleocr'
  return null
}

function editableGroup(policy: EngineLifecycleGroupPolicy): Omit<EngineLifecycleGroupPolicy, 'status'> {
  return {
    mode: policy.mode,
    ...(policy.idle_minutes == null ? {} : { idle_minutes: policy.idle_minutes }),
    ...(policy.startup_timeout_seconds == null
      ? {}
      : { startup_timeout_seconds: policy.startup_timeout_seconds }),
  }
}

export function buildEngineLifecycleUpdate(
  current: EngineLifecycleConfig,
  group: EngineLifecycleGroup,
  draft: Omit<EngineLifecycleGroupPolicy, 'status'>,
): EngineLifecycleUpdatePayload {
  return {
    defaults: {
      idle_minutes: current.defaults.idle_minutes,
      startup_timeout_seconds: current.defaults.startup_timeout_seconds,
    },
    groups: {
      paddleocr: editableGroup(current.groups.paddleocr),
      asr: editableGroup(current.groups.asr),
      reranker: editableGroup(current.groups.reranker),
      [group]: editableGroup(draft),
    },
  }
}

export function mergeEngineLifecycleConfig(
  current: EngineLifecycleConfig,
  updated: EngineLifecycleConfig,
): EngineLifecycleConfig {
  return {
    ...updated,
    groups: {
      paddleocr: { ...updated.groups.paddleocr, status: current.groups.paddleocr.status },
      asr: { ...updated.groups.asr, status: current.groups.asr.status },
      reranker: { ...updated.groups.reranker, status: current.groups.reranker.status },
    },
  }
}
