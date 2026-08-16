(function weknoraUploadMonitorBootstrap() {
  'use strict';

  if (window.__weknoraUploadMonitor) return;

  const VERSION = '1.3.1';
  const STORAGE_KEY = 'weknora_upload_monitor_v1';
  const MAX_STORED_TASKS = 50;
  const TASK_TTL_MS = 24 * 60 * 60 * 1000;
  const UPLOAD_TIMEOUT_MS = 3 * 60 * 1000;
  const UPLOAD_PATH_RE = /^\/api\/v1\/knowledge-bases\/([^/]+)\/knowledge\/file\/?$/;
  const KB_PATH_RE = /^\/platform\/knowledge-bases\/([^/?#]+)/;
  const DOCUMENT_DETAIL_QUERY_KEY = 'knowledge_id';
  const DOCUMENT_DRAWER_ROOT_SELECTOR = '.t-drawer.doc-main-drawer';
  const DOCUMENT_DRAWER_SELECTOR = '.t-drawer.doc-main-drawer.t-drawer--open > .t-drawer__content-wrapper';
  const DOCUMENT_DRAWER_DEFAULT_WIDTH = 654;
  const MIN_DETAIL_PANEL_WIDTH = 240;
  const ACTIVE_UPLOAD = new Set(['uploading', 'receiving']);
  const LEGACY_ACCEPTED = new Set(['pending', 'processing', 'finalizing', 'completed']);
  const TERMINAL = new Set(['uploaded', 'failed', 'cancelled', 'interrupted', 'timeout']);

  const tasks = new Map();
  const elapsedNodes = new Map();
  const renderedTaskNodes = new Map();
  const announcedTaskStatuses = new Map();
  let panelOpen = false;
  let documentDetailOpen = false;
  let documentDrawerHasOpened = false;
  let documentDrawerKnowledgeBaseId = '';
  let persistTimer = 0;
  let hydrateScheduled = false;
  let documentDrawerObserver;
  let observedDocumentDrawerRoot;
  let observedDocumentDrawerContent;
  let host;
  let shadow;
  let taskList;
  let announcer;
  let badge;
  let panel;
  let toggleButton;
  let toggleLabel;

  function now() {
    return Date.now();
  }

  function createTaskId() {
    if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') {
      return globalThis.crypto.randomUUID();
    }
    return `wk-upload-${now()}-${Math.random().toString(36).slice(2)}`;
  }

  function currentKnowledgeBaseId() {
    const match = window.location.pathname.match(KB_PATH_RE);
    return match ? decodeURIComponent(match[1]) : '';
  }

  function isKnowledgeBasePage() {
    return Boolean(currentKnowledgeBaseId());
  }

  function hasDocumentDetailQuery() {
    const url = normalizeURL(window.location.href);
    return Boolean(url && url.searchParams.get(DOCUMENT_DETAIL_QUERY_KEY));
  }

  function openDocumentDrawer() {
    return document.querySelector(DOCUMENT_DRAWER_SELECTOR);
  }

  function isDocumentDetailView(drawer = openDocumentDrawer()) {
    const knowledgeBaseId = currentKnowledgeBaseId();
    if (!knowledgeBaseId) {
      documentDrawerKnowledgeBaseId = '';
      documentDrawerHasOpened = false;
      return false;
    }
    if (knowledgeBaseId !== documentDrawerKnowledgeBaseId) {
      documentDrawerKnowledgeBaseId = knowledgeBaseId;
      documentDrawerHasOpened = false;
    }
    if (drawer) {
      documentDrawerHasOpened = true;
      return true;
    }
    return !documentDrawerHasOpened && hasDocumentDetailQuery();
  }

  function storedDocumentDrawerWidth() {
    const maximum = Math.min(1600, Math.max(480, Math.floor(window.innerWidth * 0.95)));
    try {
      const stored = Number.parseInt(localStorage.getItem('weknora-doc-drawer-width') || '', 10);
      if (Number.isFinite(stored) && stored > 0) return Math.max(480, Math.min(maximum, stored));
    } catch {
      // Fall back to the document drawer's public default.
    }
    return Math.max(480, Math.min(maximum, DOCUMENT_DRAWER_DEFAULT_WIDTH));
  }

  function documentDetailRailPosition(drawer) {
    const rect = drawer && typeof drawer.getBoundingClientRect === 'function'
      ? drawer.getBoundingClientRect()
      : null;
    const drawerLeft = rect && rect.width > 0
      ? rect.left
      : Math.max(0, window.innerWidth - storedDocumentDrawerWidth());
    const outsidePanelWidth = Math.min(420, drawerLeft - 28);
    if (outsidePanelWidth < MIN_DETAIL_PANEL_WIDTH) {
      return {
        anchorRight: 16,
        panelWidth: Math.max(0, window.innerWidth - 32),
      };
    }
    const railLeft = drawerLeft - 56;
    return {
      anchorRight: Math.max(8, window.innerWidth - railLeft - 44),
      panelWidth: outsidePanelWidth,
    };
  }

  function normalizeURL(value) {
    try {
      return new URL(String(value), window.location.href);
    } catch {
      return null;
    }
  }

  function uploadMatch(value) {
    const url = normalizeURL(value);
    return url ? url.pathname.match(UPLOAD_PATH_RE) : null;
  }

  function extractError(payload, fallback) {
    if (!payload) return fallback;
    if (typeof payload === 'string') return payload || fallback;
    if (typeof payload.message === 'string' && payload.message) return payload.message;
    if (typeof payload.error === 'string' && payload.error) return payload.error;
    if (payload.error && typeof payload.error.message === 'string') return payload.error.message;
    if (payload.details && typeof payload.details === 'string') return payload.details;
    return fallback;
  }

  function parseResponse(xhr) {
    if (xhr.response && typeof xhr.response === 'object') return xhr.response;
    const text = typeof xhr.responseText === 'string' ? xhr.responseText.trim() : '';
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  function responseKnowledge(payload) {
    if (!payload || typeof payload !== 'object') return null;
    const candidate = payload.data && typeof payload.data === 'object' ? payload.data : payload;
    return candidate && candidate.id ? candidate : null;
  }

  function statusPresentation(task) {
    switch (task.status) {
      case 'uploading':
        return { label: `上传中 ${Math.max(0, Math.min(100, Math.round(task.progress || 0)))}%`, tone: 'active' };
      case 'receiving':
        return { label: '服务器接收中', tone: 'active' };
      case 'uploaded':
        return { label: '已上传', tone: 'success' };
      case 'failed':
        return { label: '上传失败', tone: 'danger' };
      case 'cancelled':
        return { label: '已取消', tone: 'muted' };
      case 'timeout':
        return { label: '上传超时', tone: 'danger' };
      case 'interrupted':
        return { label: '上传中断', tone: 'danger' };
      default:
        return { label: '状态未知', tone: 'muted' };
    }
  }

  function formatSize(bytes) {
    if (!Number.isFinite(bytes) || bytes <= 0) return '大小未知';
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1024 / 1024).toFixed(2)} MB`;
  }

  function formatDuration(milliseconds) {
    const seconds = Math.max(0, Math.floor(milliseconds / 1000));
    if (seconds < 60) return `${seconds} 秒`;
    const minutes = Math.floor(seconds / 60);
    const rest = seconds % 60;
    return `${minutes} 分 ${rest} 秒`;
  }

  function taskElapsed(task) {
    const end = task.finishedAt || now();
    return formatDuration(end - task.startedAt);
  }

  function isTerminal(task) {
    return TERMINAL.has(task.status);
  }

  function sanitizeTaskForStorage(task) {
    return {
      id: task.id,
      knowledgeId: task.knowledgeId || '',
      kbId: task.kbId || '',
      name: task.name || '未知文件',
      size: Number(task.size) || 0,
      status: task.status,
      progress: Number(task.progress) || 0,
      error: task.error || '',
      startedAt: Number(task.startedAt) || now(),
      updatedAt: Number(task.updatedAt) || now(),
      finishedAt: Number(task.finishedAt) || 0,
      source: task.source || 'upload',
    };
  }

  function schedulePersist() {
    window.clearTimeout(persistTimer);
    persistTimer = window.setTimeout(() => {
      const cutoff = now() - TASK_TTL_MS;
      const stored = Array.from(tasks.values())
        .filter((task) => task.updatedAt >= cutoff)
        .sort((a, b) => b.startedAt - a.startedAt)
        .slice(0, MAX_STORED_TASKS)
        .map(sanitizeTaskForStorage);
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(stored));
      } catch {
        // The monitor is non-critical; storage denial must not affect WeKnora.
      }
    }, 120);
  }

  function loadStoredTasks() {
    let stored = [];
    try {
      stored = JSON.parse(localStorage.getItem(STORAGE_KEY) || '[]');
    } catch {
      stored = [];
    }
    if (!Array.isArray(stored)) return;
    const cutoff = now() - TASK_TTL_MS;
    for (const item of stored) {
      if (!item || !item.id || Number(item.updatedAt) < cutoff) continue;
      const task = { ...item };
      if (ACTIVE_UPLOAD.has(task.status) && !task.knowledgeId) {
        task.status = 'interrupted';
        task.error = task.error || '页面刷新或连接中断，无法确认文件是否上传完成';
        task.finishedAt = task.finishedAt || now();
      } else if (task.knowledgeId || LEGACY_ACCEPTED.has(task.status)) {
        task.status = 'uploaded';
        task.progress = 100;
        task.error = '';
        task.finishedAt = task.finishedAt || task.updatedAt || now();
      }
      tasks.set(task.id, task);
    }
  }

  function updateTask(task, patch) {
    const previousStatus = task.status;
    Object.assign(task, patch, { updatedAt: now() });
    tasks.set(task.id, task);
    schedulePersist();
    render();
    if (task.status !== previousStatus) announceTaskStatus(task);
  }

  function updateRenderedTaskProgress(task) {
    const nodes = renderedTaskNodes.get(task.id);
    if (!nodes) {
      render();
      return;
    }
    const presentation = statusPresentation(task);
    nodes.state.className = `task__state task__state--${presentation.tone}`;
    nodes.state.textContent = presentation.label;
    nodes.progress.className = `progress ${task.status === 'receiving' ? 'progress--indeterminate' : ''}`;
    nodes.progressValue.className = `progress__value progress__value--${presentation.tone}`;
    if (task.status === 'uploading') {
      nodes.progressValue.style.width = `${Math.max(2, Math.min(100, task.progress || 0))}%`;
    } else if (task.status === 'receiving') {
      nodes.progressValue.style.width = '42%';
    } else {
      nodes.progressValue.style.width = '100%';
    }
  }

  function updateUploadProgress(task, progress) {
    const status = progress >= 100 ? 'receiving' : 'uploading';
    if (status !== task.status) {
      updateTask(task, { progress, status });
      return;
    }
    Object.assign(task, { progress, updatedAt: now() });
    tasks.set(task.id, task);
    schedulePersist();
    updateRenderedTaskProgress(task);
  }

  function announceTaskStatus(task) {
    if (!announcer || announcedTaskStatuses.get(task.id) === task.status) return;
    announcedTaskStatuses.set(task.id, task.status);
    announcer.textContent = `${task.name}：${statusPresentation(task).label}`;
  }

  function applyKnowledgeToTask(task, knowledge) {
    updateTask(task, {
      knowledgeId: knowledge.id,
      name: knowledge.file_name || knowledge.title || task.name,
      size: Number(knowledge.file_size) || task.size,
      status: 'uploaded',
      progress: 100,
      error: '',
      finishedAt: now(),
    });
  }

  function hydrateInFlightTasks() {
    bindDocumentDrawerObserver();
    syncDocumentDetailSurface();
  }

  function scheduleHydrate() {
    if (hydrateScheduled) return;
    hydrateScheduled = true;
    const run = () => {
      hydrateScheduled = false;
      hydrateInFlightTasks();
    };
    if (typeof window.requestAnimationFrame === 'function') {
      window.requestAnimationFrame(run);
    } else {
      window.setTimeout(run, 0);
    }
  }

  function createUploadTask(xhr, body, kbId) {
    const file = body instanceof FormData ? body.get('file') : null;
    const customName = body instanceof FormData ? body.get('fileName') : '';
    const task = {
      id: createTaskId(),
      knowledgeId: '',
      kbId,
      name: typeof customName === 'string' && customName ? customName : (file && file.name ? file.name : '未知文件'),
      size: file && Number(file.size) ? Number(file.size) : 0,
      status: 'uploading',
      progress: 0,
      error: '',
      startedAt: now(),
      updatedAt: now(),
      finishedAt: 0,
      source: 'upload',
      timeoutMs: Number(xhr.timeout) || 0,
    };
    tasks.set(task.id, task);
    panelOpen = !documentDetailOpen;
    schedulePersist();
    render();
    announceTaskStatus(task);
    return task;
  }

  function finishUploadFailure(task, status, message) {
    updateTask(task, {
      status,
      error: message,
      finishedAt: now(),
    });
    panelOpen = !documentDetailOpen;
    render();
  }

  function instrumentXMLHttpRequest() {
    const OriginalXHR = window.XMLHttpRequest;
    if (!OriginalXHR || OriginalXHR.prototype.__weknoraUploadMonitorPatched) return;
    const originalOpen = OriginalXHR.prototype.open;
    const originalSend = OriginalXHR.prototype.send;

    OriginalXHR.prototype.open = function patchedOpen(method, url) {
      const match = uploadMatch(url);
      this.__weknoraUploadRequest = match && String(method).toUpperCase() === 'POST'
        ? { kbId: decodeURIComponent(match[1]), url: String(url) }
        : null;
      return originalOpen.apply(this, arguments);
    };

    OriginalXHR.prototype.send = function patchedSend(body) {
      const request = this.__weknoraUploadRequest;
      if (!request) return originalSend.apply(this, arguments);

      const xhr = this;
      // Keep the local upload timeout independent from the upstream frontend
      // bundle. Axios currently defaults to 30 seconds; this overlay raises
      // knowledge-file uploads only, without changing other API requests.
      if (!xhr.timeout || xhr.timeout < UPLOAD_TIMEOUT_MS) {
        xhr.timeout = UPLOAD_TIMEOUT_MS;
      }
      const task = createUploadTask(xhr, body, request.kbId);
      let settled = false;

      if (xhr.upload) {
        xhr.upload.addEventListener('progress', (event) => {
          if (settled) return;
          if (event.lengthComputable && event.total > 0) {
            const progress = Math.min(100, (event.loaded / event.total) * 100);
            updateUploadProgress(task, progress);
          }
        });
      }

      xhr.addEventListener('load', () => {
        if (settled) return;
        settled = true;
        const payload = parseResponse(xhr);
        if (xhr.status >= 200 && xhr.status < 300) {
          const knowledge = responseKnowledge(payload);
          if (knowledge) {
            applyKnowledgeToTask(task, knowledge);
          } else {
            updateTask(task, {
              status: 'uploaded',
              progress: 100,
              error: '',
              finishedAt: now(),
            });
          }
          return;
        }
        finishUploadFailure(task, 'failed', extractError(payload, `上传失败（HTTP ${xhr.status || 0}）`));
      });

      xhr.addEventListener('timeout', () => {
        if (settled) return;
        settled = true;
        const timeoutText = task.timeoutMs ? `上传超过 ${formatDuration(task.timeoutMs)}，请求已超时` : '上传请求超时';
        finishUploadFailure(task, 'timeout', timeoutText);
      });

      xhr.addEventListener('abort', () => {
        if (settled) return;
        settled = true;
        finishUploadFailure(task, 'interrupted', '上传请求被页面或浏览器中断');
      });

      xhr.addEventListener('error', () => {
        if (settled) return;
        settled = true;
        finishUploadFailure(task, 'interrupted', '上传连接异常中断');
      });

      return originalSend.apply(this, arguments);
    };

    Object.defineProperty(OriginalXHR.prototype, '__weknoraUploadMonitorPatched', {
      value: true,
      configurable: false,
      enumerable: false,
      writable: false,
    });
  }

  function installNavigationHooks() {
    for (const method of ['pushState', 'replaceState']) {
      const original = history[method];
      history[method] = function patchedHistoryMethod() {
        const result = original.apply(this, arguments);
        scheduleHydrate();
        return result;
      };
    }
    window.addEventListener('popstate', scheduleHydrate);
    window.addEventListener('knowledgeFileUploaded', scheduleHydrate);
  }

  function activeCount() {
    let count = 0;
    for (const task of tasks.values()) {
      if (!isTerminal(task)) count += 1;
    }
    return count;
  }

  function updateVisibility() {
    if (!host) return;
    const visibleOnCurrentPage = isKnowledgeBasePage() || tasks.size > 0;
    const hiddenBehindDocumentDetail = documentDetailOpen && activeCount() === 0;
    host.style.display = visibleOnCurrentPage && !hiddenBehindDocumentDetail ? 'block' : 'none';
  }

  function syncDocumentDetailSurface() {
    if (!host || !toggleButton || !toggleLabel) return;
    const drawer = openDocumentDrawer();
    const nextDocumentDetailOpen = isDocumentDetailView(drawer);
    const enteringDocumentDetail = nextDocumentDetailOpen && !documentDetailOpen;
    documentDetailOpen = nextDocumentDetailOpen;

    host.dataset.surface = documentDetailOpen ? 'document-detail' : 'knowledge-base';
    toggleButton.dataset.mode = documentDetailOpen ? 'rail' : 'default';
    toggleLabel.hidden = documentDetailOpen;

    if (documentDetailOpen) {
      const position = documentDetailRailPosition(drawer);
      host.style.setProperty('--wk-detail-anchor-right', `${position.anchorRight}px`);
      host.style.setProperty('--wk-detail-panel-width', `${position.panelWidth}px`);
      if (enteringDocumentDetail) panelOpen = false;
    } else {
      host.style.removeProperty('--wk-detail-anchor-right');
      host.style.removeProperty('--wk-detail-panel-width');
    }

    updateVisibility();
  }

  function installDocumentDetailSync() {
    documentDrawerObserver = new MutationObserver(scheduleHydrate);
    const bodyObserver = new MutationObserver(scheduleHydrate);
    bodyObserver.observe(document.body, {
      childList: true,
      subtree: true,
    });
    bindDocumentDrawerObserver();
    window.addEventListener('resize', scheduleHydrate);
    syncDocumentDetailSurface();
  }

  function bindDocumentDrawerObserver() {
    if (!documentDrawerObserver) return;
    const drawerRoot = document.querySelector(DOCUMENT_DRAWER_ROOT_SELECTOR);
    const drawerContent = openDocumentDrawer();
    if (
      drawerRoot === observedDocumentDrawerRoot
      && drawerContent === observedDocumentDrawerContent
    ) return;
    documentDrawerObserver.disconnect();
    observedDocumentDrawerRoot = drawerRoot;
    observedDocumentDrawerContent = drawerContent;
    if (drawerRoot) {
      documentDrawerObserver.observe(drawerRoot, {
        attributes: true,
        attributeFilter: ['class', 'style'],
      });
    }
    if (drawerContent && drawerContent !== drawerRoot) {
      documentDrawerObserver.observe(drawerContent, {
        attributes: true,
        attributeFilter: ['class', 'style'],
      });
    }
  }

  function createElement(tag, className, text) {
    const element = document.createElement(tag);
    if (className) element.className = className;
    if (text !== undefined) element.textContent = text;
    return element;
  }

  function removeTask(task) {
    tasks.delete(task.id);
    renderedTaskNodes.delete(task.id);
    announcedTaskStatuses.delete(task.id);
    schedulePersist();
    render();
  }

  function renderTask(task) {
    const presentation = statusPresentation(task);
    const row = createElement('article', `task task--${presentation.tone}`);
    row.dataset.taskId = task.id;

    const top = createElement('div', 'task__top');
    const name = createElement('div', 'task__name', task.name);
    name.title = task.name;
    const state = createElement('span', `task__state task__state--${presentation.tone}`, presentation.label);
    top.append(name, state);

    const meta = createElement('div', 'task__meta');
    const elapsed = createElement('span', '', `耗时 ${taskElapsed(task)}`);
    elapsed.setAttribute('aria-hidden', 'true');
    elapsedNodes.set(task.id, elapsed);
    meta.append(
      createElement('span', '', formatSize(task.size)),
      elapsed,
    );

    const progress = createElement('div', `progress ${task.status === 'receiving' ? 'progress--indeterminate' : ''}`);
    const progressValue = createElement('div', `progress__value progress__value--${presentation.tone}`);
    if (task.status === 'uploading') {
      progressValue.style.width = `${Math.max(2, Math.min(100, task.progress || 0))}%`;
    } else if (task.status === 'receiving') {
      progressValue.style.width = '42%';
    } else {
      progressValue.style.width = '100%';
    }
    progress.append(progressValue);
    renderedTaskNodes.set(task.id, { state, progress, progressValue });

    row.append(top, meta, progress);

    if (task.error) {
      row.append(createElement('div', 'task__error', task.error));
    }

    if (isTerminal(task)) {
      const actions = createElement('div', 'task__actions');
      const dismiss = createElement('button', 'text-button', '移除');
      dismiss.type = 'button';
      dismiss.addEventListener('click', () => removeTask(task));
      actions.append(dismiss);
      row.append(actions);
    }

    return row;
  }

  function render() {
    if (!taskList) return;
    const ordered = Array.from(tasks.values()).sort((a, b) => b.startedAt - a.startedAt);
    elapsedNodes.clear();
    renderedTaskNodes.clear();
    taskList.replaceChildren();
    if (ordered.length === 0) {
      const empty = createElement('div', 'empty');
      empty.append(
        createElement('div', 'empty__title', '暂无上传任务'),
        createElement('div', 'empty__desc', '选择文件后会在这里显示实时上传进度与结果。'),
      );
      taskList.append(empty);
    } else {
      for (const task of ordered) taskList.append(renderTask(task));
    }

    const count = activeCount();
    badge.textContent = String(count);
    badge.hidden = count === 0;
    toggleButton.classList.toggle('toggle--active', count > 0);
    panel.hidden = !panelOpen;
    toggleButton.setAttribute('aria-expanded', String(panelOpen));
    toggleButton.setAttribute(
      'aria-label',
      documentDetailOpen
        ? `打开文件上传任务列表，${count} 个活动任务`
        : '打开文件上传任务列表',
    );
    updateVisibility();
  }

  function refreshElapsedTimes() {
    for (const task of tasks.values()) {
      if (isTerminal(task)) continue;
      const elapsed = elapsedNodes.get(task.id);
      if (elapsed) elapsed.textContent = `耗时 ${taskElapsed(task)}`;
    }
  }

  function syncTheme() {
    if (!host) return;
    host.dataset.theme = document.documentElement.getAttribute('theme-mode') === 'dark' ? 'dark' : 'light';
  }

  function installThemeSync() {
    const observer = new MutationObserver(syncTheme);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['theme-mode'],
    });
    syncTheme();
  }

  function mountUI() {
    host = document.createElement('div');
    host.id = 'weknora-upload-monitor-host';
    host.dataset.version = VERSION;
    shadow = host.attachShadow({ mode: 'open' });
    shadow.innerHTML = `
      <style>
        :host {
          all: initial;
          color-scheme: light;
          --wk-root-text: #18211b;
          --wk-strong-text: #1f2b23;
          --wk-title-text: #33463a;
          --wk-secondary-text: #6d786f;
          --wk-meta-text: #7b857e;
          --wk-subdued-text: #859088;
          --wk-toggle-bg: #fff;
          --wk-toggle-bg-hover: #f7fbf8;
          --wk-toggle-border: rgba(28, 77, 47, .18);
          --wk-toggle-text: #1f5134;
          --wk-toggle-shadow: 0 12px 34px rgba(23, 49, 33, .18);
          --wk-active-dot: #25a55f;
          --wk-active-ring: rgba(37, 165, 95, .12);
          --wk-badge-bg: #1f7145;
          --wk-badge-text: #fff;
          --wk-panel-bg: rgba(252, 254, 252, .98);
          --wk-panel-border: rgba(28, 77, 47, .14);
          --wk-panel-shadow: 0 22px 60px rgba(20, 44, 29, .24);
          --wk-divider: #e8eee9;
          --wk-button-bg: #edf3ef;
          --wk-button-bg-hover: #e3ece6;
          --wk-button-text: #375342;
          --wk-task-bg: #fff;
          --wk-task-border: #e5ece7;
          --wk-danger-task-bg: #fffafa;
          --wk-danger-task-border: #f2d2d2;
          --wk-active-state-bg: #e7f6ed;
          --wk-active-state-text: #176b3a;
          --wk-success-state-bg: #eaf5ed;
          --wk-success-state-text: #23613a;
          --wk-danger-state-bg: #fdeaea;
          --wk-danger-state-text: #aa3030;
          --wk-muted-state-bg: #eef0ef;
          --wk-muted-state-text: #66706a;
          --wk-progress-track: #edf1ee;
          --wk-progress-active: #2c9c5b;
          --wk-progress-success: #4c9567;
          --wk-progress-danger: #d95a5a;
          --wk-progress-muted: #9da7a0;
          --wk-error-bg: #fff0f0;
          --wk-error-text: #9c3030;
          --wk-text-button: #5b6e61;
          --wk-text-button-hover: #1f7145;
          --wk-focus-ring: rgba(31, 113, 69, .35);
        }
        :host([data-theme="dark"]) {
          color-scheme: dark;
          --wk-root-text: rgba(255, 255, 255, .9);
          --wk-strong-text: rgba(255, 255, 255, .9);
          --wk-title-text: rgba(255, 255, 255, .9);
          --wk-secondary-text: rgba(255, 255, 255, .55);
          --wk-meta-text: rgba(255, 255, 255, .55);
          --wk-subdued-text: rgba(255, 255, 255, .42);
          --wk-toggle-bg: #242424;
          --wk-toggle-bg-hover: #2c2c2c;
          --wk-toggle-border: rgba(7, 192, 95, .25);
          --wk-toggle-text: #46bf96;
          --wk-toggle-shadow: 0 12px 34px rgba(0, 0, 0, .42);
          --wk-active-dot: #07c05f;
          --wk-active-ring: rgba(7, 192, 95, .2);
          --wk-badge-bg: #06b04d;
          --wk-badge-text: #fff;
          --wk-panel-bg: rgba(36, 36, 36, .98);
          --wk-panel-border: rgba(255, 255, 255, .12);
          --wk-panel-shadow: 0 22px 60px rgba(0, 0, 0, .52);
          --wk-divider: #383838;
          --wk-button-bg: #2c2c2c;
          --wk-button-bg-hover: #383838;
          --wk-button-text: rgba(255, 255, 255, .75);
          --wk-task-bg: #2c2c2c;
          --wk-task-border: #383838;
          --wk-danger-task-bg: #321e20;
          --wk-danger-task-border: #703439;
          --wk-active-state-bg: rgba(6, 176, 77, .18);
          --wk-active-state-text: #46bf96;
          --wk-success-state-bg: #193a2a;
          --wk-success-state-text: #80d2b6;
          --wk-danger-state-bg: #472324;
          --wk-danger-state-text: #ec888e;
          --wk-muted-state-bg: #383838;
          --wk-muted-state-text: rgba(255, 255, 255, .55);
          --wk-progress-track: #383838;
          --wk-progress-active: #07c05f;
          --wk-progress-success: #43af8a;
          --wk-progress-danger: #de6670;
          --wk-progress-muted: #777;
          --wk-error-bg: #472324;
          --wk-error-text: #edb1b6;
          --wk-text-button: rgba(255, 255, 255, .55);
          --wk-text-button-hover: #46bf96;
          --wk-focus-ring: rgba(70, 191, 150, .5);
        }
        *, *::before, *::after { box-sizing: border-box; }
        .root { position: fixed; right: 22px; bottom: 22px; z-index: 1800; font-family: Inter, "Microsoft YaHei", system-ui, sans-serif; color: var(--wk-root-text); }
        :host([data-surface="document-detail"]) .root { right: var(--wk-detail-anchor-right, 22px); }
        .toggle { position: relative; display: inline-flex; float: right; align-items: center; gap: 8px; height: 44px; padding: 0 16px; border: 1px solid var(--wk-toggle-border); border-radius: 999px; background: var(--wk-toggle-bg); color: var(--wk-toggle-text); box-shadow: var(--wk-toggle-shadow); cursor: pointer; font: 600 14px/1 inherit; }
        :host([data-surface="document-detail"]) .toggle { width: 44px; padding: 0; justify-content: center; }
        :host([data-surface="document-detail"]) .toggle--active::before { display: none; }
        .toggle:hover { background: var(--wk-toggle-bg-hover); }
        .toggle:focus-visible, .icon-button:focus-visible, .text-button:focus-visible { outline: 3px solid var(--wk-focus-ring); outline-offset: 2px; }
        .toggle--active::before { content: ""; width: 8px; height: 8px; border-radius: 50%; background: var(--wk-active-dot); box-shadow: 0 0 0 4px var(--wk-active-ring); }
        .badge { display: inline-grid; min-width: 20px; height: 20px; padding: 0 5px; place-items: center; border-radius: 999px; background: var(--wk-badge-bg); color: var(--wk-badge-text); font-size: 11px; }
        .panel { width: min(420px, calc(100vw - 32px), var(--wk-detail-panel-width, 420px)); max-height: min(620px, calc(100vh - 96px)); margin-bottom: 12px; overflow: hidden; border: 1px solid var(--wk-panel-border); border-radius: 18px; background: var(--wk-panel-bg); box-shadow: var(--wk-panel-shadow); backdrop-filter: blur(16px); }
        .panel[hidden] { display: none; }
        .header { display: flex; align-items: center; justify-content: space-between; padding: 16px 16px 12px; border-bottom: 1px solid var(--wk-divider); }
        .header__title { font-size: 16px; font-weight: 750; letter-spacing: -.01em; }
        .header__desc { margin-top: 4px; color: var(--wk-secondary-text); font-size: 11px; }
        .header__actions { display: flex; gap: 8px; }
        .icon-button { width: 30px; height: 30px; border: 0; border-radius: 9px; background: var(--wk-button-bg); color: var(--wk-button-text); cursor: pointer; font: 600 15px/1 inherit; }
        .icon-button:hover { background: var(--wk-button-bg-hover); }
        .list { max-height: min(520px, calc(100vh - 190px)); padding: 10px; overflow: auto; }
        .task { padding: 12px; border: 1px solid var(--wk-task-border); border-radius: 13px; background: var(--wk-task-bg); }
        .task + .task { margin-top: 8px; }
        .task--danger { border-color: var(--wk-danger-task-border); background: var(--wk-danger-task-bg); }
        .task__top { display: flex; align-items: flex-start; gap: 10px; }
        .task__name { min-width: 0; flex: 1; overflow: hidden; color: var(--wk-strong-text); font-size: 13px; font-weight: 650; text-overflow: ellipsis; white-space: nowrap; }
        .task__state { flex: none; padding: 3px 7px; border-radius: 999px; font-size: 10px; font-weight: 700; }
        .task__state--active { background: var(--wk-active-state-bg); color: var(--wk-active-state-text); }
        .task__state--success { background: var(--wk-success-state-bg); color: var(--wk-success-state-text); }
        .task__state--danger { background: var(--wk-danger-state-bg); color: var(--wk-danger-state-text); }
        .task__state--muted { background: var(--wk-muted-state-bg); color: var(--wk-muted-state-text); }
        .task__meta { display: flex; gap: 12px; margin-top: 6px; color: var(--wk-meta-text); font-size: 10px; }
        .progress { height: 5px; margin-top: 10px; overflow: hidden; border-radius: 999px; background: var(--wk-progress-track); }
        .progress__value { height: 100%; border-radius: inherit; transition: width .2s ease; }
        .progress__value--active { background: var(--wk-progress-active); }
        .progress__value--success { background: var(--wk-progress-success); }
        .progress__value--danger { background: var(--wk-progress-danger); }
        .progress__value--muted { background: var(--wk-progress-muted); }
        .progress--indeterminate .progress__value { animation: slide 1.35s ease-in-out infinite; }
        .task__error { margin-top: 9px; padding: 8px 9px; border-radius: 8px; background: var(--wk-error-bg); color: var(--wk-error-text); font-size: 11px; line-height: 1.45; word-break: break-word; }
        .task__actions { display: flex; justify-content: flex-end; margin-top: 6px; }
        .text-button { border: 0; background: transparent; color: var(--wk-text-button); cursor: pointer; font: 600 11px/1 inherit; }
        .text-button:hover { color: var(--wk-text-button-hover); }
        .empty { padding: 30px 18px; text-align: center; }
        .empty__title { color: var(--wk-title-text); font-size: 13px; font-weight: 650; }
        .empty__desc { margin-top: 7px; color: var(--wk-subdued-text); font-size: 11px; line-height: 1.5; }
        .announcer { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
        @keyframes slide { 0% { transform: translateX(-115%); } 55%, 100% { transform: translateX(245%); } }
        @media (max-width: 640px) { .root { right: 16px; bottom: 16px; } }
        @media (prefers-reduced-motion: reduce) { .progress--indeterminate .progress__value { animation: none; width: 100% !important; opacity: .65; } }
      </style>
      <div class="root">
        <section class="panel" hidden aria-label="上传任务列表">
          <header class="header">
            <div>
              <div class="header__title">文件上传任务</div>
              <div class="header__desc">独立外挂 · 仅显示上传传输状态</div>
            </div>
            <div class="header__actions">
              <button class="icon-button clear" type="button" title="清除已上传和失败记录" aria-label="清除已上传和失败记录">🗑</button>
              <button class="icon-button minimize" type="button" title="最小化" aria-label="最小化">−</button>
            </div>
          </header>
          <div class="list"></div>
        </section>
        <div class="announcer" role="status" aria-live="polite" aria-atomic="true"></div>
        <button class="toggle" type="button" aria-expanded="false" aria-label="打开文件上传任务列表">
          <span class="toggle-label">上传任务</span><span class="badge" hidden>0</span>
        </button>
      </div>`;

    panel = shadow.querySelector('.panel');
    taskList = shadow.querySelector('.list');
    announcer = shadow.querySelector('.announcer');
    badge = shadow.querySelector('.badge');
    toggleButton = shadow.querySelector('.toggle');
    toggleLabel = shadow.querySelector('.toggle-label');
    toggleButton.addEventListener('click', () => {
      panelOpen = !panelOpen;
      render();
    });
    shadow.querySelector('.minimize').addEventListener('click', () => {
      panelOpen = false;
      render();
      toggleButton.focus();
    });
    shadow.querySelector('.clear').addEventListener('click', () => {
      for (const task of Array.from(tasks.values())) {
        if (isTerminal(task)) removeTask(task);
      }
      render();
    });
    document.documentElement.append(host);
    installThemeSync();
    installDocumentDetailSync();
    render();
  }

  loadStoredTasks();
  instrumentXMLHttpRequest();
  installNavigationHooks();

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', () => {
      mountUI();
      scheduleHydrate();
    }, { once: true });
  } else {
    mountUI();
    scheduleHydrate();
  }

  window.setInterval(() => {
    if (panelOpen) refreshElapsedTimes();
  }, 1000);

  window.__weknoraUploadMonitor = Object.freeze({
    version: VERSION,
    open() {
      panelOpen = true;
      render();
    },
    refresh: scheduleHydrate,
  });
})();
