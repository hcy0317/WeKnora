export interface LatestKeyedRequestCoordinator<K> {
  fetch: (key: K, force?: boolean) => Promise<void>
  invalidate: () => void
}

/**
 * Serializes reads whose result belongs to one selected key.
 *
 * Switching keys queues the new read behind the active one, while a revision
 * guard prevents that older read from publishing after the selection changed.
 * Repeated non-forced reads for the latest key share the same queued task.
 */
export function createLatestKeyedRequestCoordinator<K, V>(
  request: (key: K) => Promise<V>,
  apply: (key: K, value: V) => void,
): LatestKeyedRequestCoordinator<K> {
  let revision = 0
  let latestKey: K | undefined
  let latestTask: Promise<void> | null = null
  let queueTail: Promise<void> | null = null

  const fetch = (key: K, force = false): Promise<void> => {
    if (!force && latestTask && Object.is(key, latestKey)) return latestTask

    latestKey = key
    const requestRevision = ++revision
    const run = async () => {
      const value = await request(key)
      if (requestRevision === revision) apply(key, value)
    }
    const task = queueTail ? queueTail.catch(() => {}).then(run) : run()

    queueTail = task
    latestTask = task
    void task.then(
      () => {
        if (latestTask === task) latestTask = null
      },
      () => {
        if (latestTask === task) latestTask = null
      },
    )
    return task
  }

  return {
    fetch,
    invalidate: () => {
      revision += 1
      latestKey = undefined
    },
  }
}
