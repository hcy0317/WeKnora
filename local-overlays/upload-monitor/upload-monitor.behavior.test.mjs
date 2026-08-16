import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const monitorSource = readFileSync(new URL('./upload-monitor.js', import.meta.url), 'utf8');
const monitorConfig = readFileSync(new URL('./upload-monitor.conf', import.meta.url), 'utf8');
const monitorReadme = readFileSync(new URL('./README.md', import.meta.url), 'utf8');
const uploadConfirmSource = readFileSync(
  new URL('../../frontend/src/views/knowledge/components/UploadConfirmDialog.vue', import.meta.url),
  'utf8',
);
const documentDetailSource = readFileSync(
  new URL('../../frontend/src/components/doc-content.vue', import.meta.url),
  'utf8',
);

class MemoryStorage {
  #values = new Map();

  getItem(key) {
    return this.#values.get(key) ?? null;
  }

  setItem(key, value) {
    this.#values.set(key, String(value));
  }
}

class FakeStyle {
  #values = new Map();

  display = '';

  setProperty(name, value) {
    this.#values.set(name, String(value));
  }

  getPropertyValue(name) {
    return this.#values.get(name) ?? '';
  }

  removeProperty(name) {
    this.#values.delete(name);
  }
}

class FakeClassList {
  #values = new Set();

  toggle(name, force) {
    if (force === true) this.#values.add(name);
    else if (force === false) this.#values.delete(name);
    else if (this.#values.has(name)) this.#values.delete(name);
    else this.#values.add(name);
    return this.#values.has(name);
  }

  contains(name) {
    return this.#values.has(name);
  }
}

class FakeElement extends EventTarget {
  constructor(tagName, ownerDocument) {
    super();
    this.tagName = tagName.toUpperCase();
    this.ownerDocument = ownerDocument;
    this.children = [];
    this.dataset = {};
    this.style = new FakeStyle();
    this.classList = new FakeClassList();
    this.attributes = new Map();
    this.hidden = false;
    this.textContent = '';
    this.className = '';
    this.id = '';
    this.replaceChildrenCount = 0;
  }

  append(...children) {
    for (const child of children) child.parentNode = this;
    this.children.push(...children);
  }

  replaceChildren(...children) {
    this.replaceChildrenCount += 1;
    for (const child of children) child.parentNode = this;
    this.children = [...children];
  }

  setAttribute(name, value) {
    this.attributes.set(name, String(value));
  }

  getAttribute(name) {
    return this.attributes.get(name) ?? null;
  }

  focus() {
    this.ownerDocument.activeElement = this;
  }

  attachShadow() {
    this.shadowRoot = new FakeShadowRoot(this.ownerDocument);
    return this.shadowRoot;
  }
}

class FakeShadowRoot {
  constructor(ownerDocument) {
    this.ownerDocument = ownerDocument;
    this.nodes = new Map();
    this.markup = '';
  }

  set innerHTML(value) {
    this.markup = value;
    const root = new FakeElement('div', this.ownerDocument);
    const panel = new FakeElement('section', this.ownerDocument);
    const list = new FakeElement('div', this.ownerDocument);
    const listMarkup = value.match(/<div class="list"([^>]*)>/)?.[1] ?? '';
    const listLive = listMarkup.match(/aria-live="([^"]+)"/)?.[1];
    if (listLive) list.setAttribute('aria-live', listLive);
    const announcerMarkup = value.match(/<div class="announcer"([^>]*)>/)?.[1] ?? '';
    const announcer = announcerMarkup ? new FakeElement('div', this.ownerDocument) : null;
    if (announcer) {
      const role = announcerMarkup.match(/role="([^"]+)"/)?.[1];
      const live = announcerMarkup.match(/aria-live="([^"]+)"/)?.[1];
      const atomic = announcerMarkup.match(/aria-atomic="([^"]+)"/)?.[1];
      if (role) announcer.setAttribute('role', role);
      if (live) announcer.setAttribute('aria-live', live);
      if (atomic) announcer.setAttribute('aria-atomic', atomic);
    }
    const badge = new FakeElement('span', this.ownerDocument);
    const toggleLabel = new FakeElement('span', this.ownerDocument);
    const toggle = new FakeElement('button', this.ownerDocument);
    const minimize = new FakeElement('button', this.ownerDocument);
    const clear = new FakeElement('button', this.ownerDocument);
    panel.hidden = true;
    badge.hidden = true;
    toggleLabel.textContent = '上传任务';
    toggle.append(toggleLabel, badge);
    panel.append(list, minimize, clear);
    root.append(panel, ...(announcer ? [announcer] : []), toggle);
    this.nodes = new Map([
      ['.root', root],
      ['.panel', panel],
      ['.list', list],
      ['.badge', badge],
      ['.toggle-label', toggleLabel],
      ['.toggle', toggle],
      ['.minimize', minimize],
      ['.clear', clear],
    ]);
    if (announcer) this.nodes.set('.announcer', announcer);
  }

  get innerHTML() {
    return this.markup;
  }

  querySelector(selector) {
    return this.nodes.get(selector) ?? null;
  }
}

class FakeDocument extends EventTarget {
  constructor() {
    super();
    this.readyState = 'complete';
    this.documentElement = new FakeElement('html', this);
    this.body = new FakeElement('body', this);
    this.activeElement = null;
    this.detailDrawer = null;
  }

  createElement(tagName) {
    return new FakeElement(tagName, this);
  }

  getElementById(id) {
    return this.documentElement.children.find((child) => child.id === id) ?? null;
  }

  querySelector(selector) {
    if (selector === '.t-drawer.doc-main-drawer.t-drawer--open > .t-drawer__content-wrapper') {
      return this.detailDrawer;
    }
    if (selector === '.t-drawer.doc-main-drawer') return this.detailDrawerRoot;
    return null;
  }
}

class FakeFormData {
  #values = new Map();

  set(name, value) {
    this.#values.set(name, value);
  }

  get(name) {
    return this.#values.get(name) ?? null;
  }
}

function textOf(node) {
  return [node.textContent, ...node.children.flatMap((child) => textOf(child))].join(' ');
}

function createBrowser(
  path = '/platform/knowledge-bases/kb-1?knowledge_id=doc-1',
  { innerWidth = 1280, drawerRect, drawerOpen } = {},
) {
  const document = new FakeDocument();
  const localStorage = new MemoryStorage();
  const url = new URL(path, 'http://weknora.local');
  const mutationObservers = [];
  const intervalCallbacks = [];
  const resolvedDrawerOpen = drawerOpen ?? url.searchParams.has('knowledge_id');
  const resolvedDrawerRect = drawerRect ?? {
    left: innerWidth - 654,
    right: innerWidth,
    top: 0,
    bottom: 720,
    width: 654,
    height: 720,
  };
  document.detailDrawer = resolvedDrawerOpen
    ? { getBoundingClientRect: () => resolvedDrawerRect }
    : null;
  document.detailDrawerRoot = { nodeName: 'DIV' };

  class FakeMutationObserver {
    constructor(callback) {
      this.callback = callback;
      this.observations = [];
      mutationObservers.push(this);
    }

    observe(target, options) {
      this.observations.push({ target, options });
    }

    disconnect() {
      this.observations = [];
    }
  }

  class FakeXMLHttpRequest extends EventTarget {
    constructor() {
      super();
      this.upload = new EventTarget();
      this.timeout = 0;
      this.status = 0;
      this.response = null;
      this.responseText = '';
    }

    open(method, requestURL) {
      this.method = method;
      this.requestURL = requestURL;
    }

    send(body) {
      this.body = body;
    }

    emit(type) {
      this.dispatchEvent(new Event(type));
    }

    emitUploadProgress(loaded, total) {
      const event = new Event('progress');
      Object.defineProperties(event, {
        lengthComputable: { value: true },
        loaded: { value: loaded },
        total: { value: total },
      });
      this.upload.dispatchEvent(event);
    }
  }

  const windowEvents = new EventTarget();
  const applyHistoryURL = (nextURL) => {
    if (nextURL === undefined || nextURL === null) return;
    url.href = new URL(String(nextURL), url.href).href;
  };
  const history = {
    pushState(_state, _unused, nextURL) {
      applyHistoryURL(nextURL);
    },
    replaceState(_state, _unused, nextURL) {
      applyHistoryURL(nextURL);
    },
  };
  const window = {
    document,
    history,
    localStorage,
    location: url,
    XMLHttpRequest: FakeXMLHttpRequest,
    innerWidth,
    addEventListener: windowEvents.addEventListener.bind(windowEvents),
    dispatchEvent: windowEvents.dispatchEvent.bind(windowEvents),
    clearTimeout() {},
    setTimeout(callback) {
      callback();
      return 1;
    },
    setInterval(callback) {
      intervalCallbacks.push(callback);
      return intervalCallbacks.length;
    },
  };
  const context = {
    URL,
    Event,
    FormData: FakeFormData,
    MutationObserver: FakeMutationObserver,
    console,
    crypto: { randomUUID: () => 'task-1' },
    document,
    globalThis: null,
    history,
    localStorage,
    window,
  };
  context.globalThis = context;
  vm.runInNewContext(monitorSource, context, { filename: 'upload-monitor.js' });
  return {
    document,
    localStorage,
    mutationObservers,
    window,
    runIntervals() {
      for (const callback of intervalCallbacks) callback();
    },
    get host() {
      return document.getElementById('weknora-upload-monitor-host');
    },
  };
}

test('document detail keeps idle controls off the body while active uploads remain accessible', () => {
  const browser = createBrowser();
  assert.equal(browser.host.style.display, 'none', 'idle controls must not cover document detail content');

  const drawer = browser.document.detailDrawer;
  browser.document.detailDrawer = null;
  for (const observer of browser.mutationObservers) observer.callback();
  assert.equal(browser.host.style.display, 'block', 'closing the drawer restores the normal knowledge-base entry');
  browser.document.detailDrawer = drawer;
  for (const observer of browser.mutationObservers) observer.callback();
  assert.equal(browser.host.style.display, 'none');

  const form = new FakeFormData();
  form.set('file', { name: 'report.pdf', size: 1024 });
  const xhr = new browser.window.XMLHttpRequest();
  xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
  xhr.send(form);

  const badge = browser.host.shadowRoot.querySelector('.badge');
  const panel = browser.host.shadowRoot.querySelector('.panel');
  const toggle = browser.host.shadowRoot.querySelector('.toggle');
  const toggleLabel = browser.host.shadowRoot.querySelector('.toggle-label');
  assert.equal(browser.host.style.display, 'block', 'an active upload must remain accessible');
  assert.equal(browser.host.dataset.surface, 'document-detail');
  assert.equal(browser.host.style.getPropertyValue('--wk-detail-anchor-right'), '666px');
  assert.equal(browser.window.innerWidth - 666, 614, 'the rail right edge stays left of the drawer at x=626');
  assert.equal(toggle.dataset.mode, 'rail');
  assert.equal(toggleLabel.hidden, true);
  assert.equal(badge.hidden, false);
  assert.equal(badge.textContent, '1');
  assert.equal(panel.hidden, true, 'entering document detail must not auto-open a covering panel');

  toggle.dispatchEvent(new Event('click'));
  assert.equal(panel.hidden, false, 'the user can still open active task details from the rail');
});

test('cross-KB history navigation keeps delayed knowledge detail surfaces scoped to the new KB', () => {
  const browser = createBrowser('/platform/knowledge-bases/kb-a?knowledge_id=doc-a');
  const drawer = browser.document.detailDrawer;
  browser.document.detailDrawer = null;
  for (const observer of browser.mutationObservers) observer.callback();
  assert.equal(browser.host.dataset.surface, 'knowledge-base', 'a closed drawer must suppress its stale query in the same KB');

  browser.window.history.pushState({}, '', '/platform/knowledge-bases/kb-b?knowledge_id=doc-b');
  assert.equal(browser.window.location.pathname, '/platform/knowledge-bases/kb-b');
  assert.equal(browser.host.dataset.surface, 'document-detail', 'pushState must recognize the new KB before its drawer mounts');
  assert.equal(browser.host.style.display, 'none');

  browser.document.detailDrawer = drawer;
  for (const observer of browser.mutationObservers) observer.callback();
  browser.document.detailDrawer = null;
  for (const observer of browser.mutationObservers) observer.callback();
  assert.equal(browser.host.dataset.surface, 'knowledge-base');

  browser.window.history.replaceState({}, '', '/platform/knowledge-bases/kb-c?knowledge_id=doc-c');
  assert.equal(browser.window.location.pathname, '/platform/knowledge-bases/kb-c');
  assert.equal(browser.host.dataset.surface, 'document-detail', 'replaceState must reset the drawer-open history for a new KB');

  browser.document.detailDrawer = drawer;
  for (const observer of browser.mutationObservers) observer.callback();
  browser.document.detailDrawer = null;
  for (const observer of browser.mutationObservers) observer.callback();
  browser.window.location.href = new URL(
    '/platform/knowledge-bases/kb-d?knowledge_id=doc-d',
    browser.window.location.href,
  ).href;
  browser.window.dispatchEvent(new Event('popstate'));
  assert.equal(browser.window.location.pathname, '/platform/knowledge-bases/kb-d');
  assert.equal(browser.host.dataset.surface, 'document-detail', 'popstate must preserve the delayed detail surface in the destination KB');
});

test('theme changes and any 2xx upload settlement are observable without a refresh', () => {
  const browser = createBrowser('/platform/knowledge-bases/kb-1');
  browser.document.documentElement.setAttribute('theme-mode', 'dark');
  for (const observer of browser.mutationObservers) observer.callback();
  assert.equal(browser.host.dataset.theme, 'dark');

  const form = new FakeFormData();
  form.set('file', { name: 'report.pdf', size: 1024 });
  const xhr = new browser.window.XMLHttpRequest();
  xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
  xhr.send(form);
  xhr.status = 202;
  xhr.response = { data: { id: 'doc-1', parse_status: 'processing' } };
  xhr.emit('load');

  const list = browser.host.shadowRoot.querySelector('.list');
  const badge = browser.host.shadowRoot.querySelector('.badge');
  assert.match(textOf(list), /已上传/);
  assert.doesNotMatch(textOf(list), /处理中|后处理|解析/);
  assert.equal(badge.hidden, true);
});

test('upload progress updates rows in place and announces only transport state transitions', () => {
  const browser = createBrowser('/platform/knowledge-bases/kb-1');
  const form = new FakeFormData();
  form.set('file', { name: 'announced.pdf', size: 1024 });
  const xhr = new browser.window.XMLHttpRequest();
  xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
  xhr.send(form);

  const list = browser.host.shadowRoot.querySelector('.list');
  const announcer = browser.host.shadowRoot.querySelector('.announcer');
  const badge = browser.host.shadowRoot.querySelector('.badge');
  assert.equal(list.getAttribute('aria-live'), null, 'the changing row list itself must not be a live region');
  assert.ok(announcer, 'state transitions need a dedicated announcer');
  assert.equal(announcer.getAttribute('role'), 'status');
  assert.equal(announcer.getAttribute('aria-live'), 'polite');
  assert.equal(announcer.getAttribute('aria-atomic'), 'true');
  assert.equal(
    announcer.parentNode,
    browser.host.shadowRoot.querySelector('.root'),
    'the status announcer must stay outside the potentially hidden panel',
  );

  const initialRenderCount = list.replaceChildrenCount;
  const initialAnnouncement = announcer.textContent;
  xhr.emitUploadProgress(10, 100);
  xhr.emitUploadProgress(45, 100);
  xhr.emitUploadProgress(82, 100);
  assert.equal(list.replaceChildrenCount, initialRenderCount, 'percentage-only progress must update the existing row');
  assert.match(textOf(list), /上传中 82%/);
  assert.equal(announcer.textContent, initialAnnouncement, 'percentage-only progress must not repeat a live announcement');

  xhr.emitUploadProgress(100, 100);
  assert.equal(list.replaceChildrenCount, initialRenderCount + 1, 'entering server-receiving state may rebuild once');
  assert.match(announcer.textContent, /announced\.pdf.*服务器接收中/);
  const receivingRenderCount = list.replaceChildrenCount;
  const receivingAnnouncement = announcer.textContent;
  xhr.emitUploadProgress(100, 100);
  assert.equal(list.replaceChildrenCount, receivingRenderCount, 'repeated receiving progress must not rebuild the row');
  assert.equal(announcer.textContent, receivingAnnouncement, 'repeated receiving progress must not re-announce the same state');

  xhr.status = 202;
  xhr.response = { accepted: true };
  xhr.emit('load');
  assert.equal(list.replaceChildrenCount, receivingRenderCount + 1);
  assert.match(textOf(list), /已上传/);
  assert.doesNotMatch(textOf(list), /处理中|后处理|解析/);
  assert.match(announcer.textContent, /announced\.pdf.*已上传/);
  assert.equal(badge.hidden, true, 'any final 2xx ends browser transport activity');
});

test('375px full-width document detail keeps the active rail and opened panel inside the viewport', () => {
  const browser = createBrowser(undefined, {
    innerWidth: 375,
    drawerRect: { left: 0, right: 375, top: 0, bottom: 720, width: 375, height: 720 },
  });
  const form = new FakeFormData();
  form.set('file', { name: 'mobile.pdf', size: 1024 });
  const xhr = new browser.window.XMLHttpRequest();
  xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
  xhr.send(form);

  const panel = browser.host.shadowRoot.querySelector('.panel');
  const toggle = browser.host.shadowRoot.querySelector('.toggle');
  assert.equal(toggle.dataset.mode, 'rail');
  assert.equal(browser.host.style.getPropertyValue('--wk-detail-anchor-right'), '16px');
  assert.equal(browser.host.style.getPropertyValue('--wk-detail-panel-width'), '343px');

  toggle.dispatchEvent(new Event('click'));
  assert.equal(panel.hidden, false);
  const panelRight = 375 - 16;
  const panelLeft = panelRight - 343;
  assert.equal(panelLeft, 16);
  assert.equal(panelRight, 359);
});

test('800px and 1280px layouts fall back only when the measured drawer leaves too little outside space', () => {
  const startUpload = (browser, name) => {
    const form = new FakeFormData();
    form.set('file', { name, size: 1024 });
    const xhr = new browser.window.XMLHttpRequest();
    xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
    xhr.send(form);
  };

  const fullWidth = createBrowser(undefined, {
    innerWidth: 800,
    drawerRect: { left: 0, right: 800, top: 0, bottom: 720, width: 800, height: 720 },
  });
  startUpload(fullWidth, 'full-width.pdf');
  assert.equal(fullWidth.host.style.getPropertyValue('--wk-detail-anchor-right'), '16px');
  assert.equal(fullWidth.host.style.getPropertyValue('--wk-detail-panel-width'), '768px');

  const outsideSpace = createBrowser(undefined, {
    innerWidth: 800,
    drawerRect: { left: 500, right: 800, top: 0, bottom: 720, width: 300, height: 720 },
  });
  startUpload(outsideSpace, 'outside-space.pdf');
  assert.equal(outsideSpace.host.style.getPropertyValue('--wk-detail-anchor-right'), '312px');
  assert.equal(outsideSpace.host.style.getPropertyValue('--wk-detail-panel-width'), '420px');

  const desktop = createBrowser(undefined, {
    innerWidth: 1280,
    drawerRect: { left: 800, right: 1280, top: 0, bottom: 800, width: 480, height: 800 },
  });
  startUpload(desktop, 'desktop.pdf');
  assert.equal(desktop.host.style.getPropertyValue('--wk-detail-anchor-right'), '492px');
  assert.equal(desktop.host.style.getPropertyValue('--wk-detail-panel-width'), '420px');
});

test('minimizing the panel returns focus to the visible upload-task toggle', () => {
  const browser = createBrowser('/platform/knowledge-bases/kb-1');
  const form = new FakeFormData();
  form.set('file', { name: 'keyboard.pdf', size: 1024 });
  const xhr = new browser.window.XMLHttpRequest();
  xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
  xhr.send(form);

  const panel = browser.host.shadowRoot.querySelector('.panel');
  const minimize = browser.host.shadowRoot.querySelector('.minimize');
  const toggle = browser.host.shadowRoot.querySelector('.toggle');
  assert.equal(panel.hidden, false);
  minimize.focus();
  assert.equal(browser.document.activeElement, minimize);

  minimize.dispatchEvent(new Event('click'));
  assert.equal(panel.hidden, true);
  assert.equal(browser.document.activeElement, toggle);
});

test('drawer synchronization does not observe class and style churn across the whole body subtree', () => {
  const browser = createBrowser();
  const observations = browser.mutationObservers.flatMap((observer) => observer.observations);
  const bodyObservation = observations.find(({ target }) => target === browser.document.body);
  assert.ok(bodyObservation, 'the body child list must reveal teleported drawer insertion and removal');
  assert.equal(bodyObservation.options.childList, true);
  assert.equal(bodyObservation.options.subtree, true);
  assert.notEqual(bodyObservation.options.attributes, true, 'body subtree attributes are too noisy');

  const drawerObservation = observations.find(({ target }) => target === browser.document.detailDrawerRoot);
  assert.ok(drawerObservation, 'the drawer root must own its class/style observation');
  assert.equal(drawerObservation.options.attributes, true);
  assert.deepEqual([...drawerObservation.options.attributeFilter], ['class', 'style']);
  assert.notEqual(drawerObservation.options.subtree, true);

  const contentObservation = observations.find(({ target }) => target === browser.document.detailDrawer);
  assert.ok(contentObservation, 'the live content wrapper must expose width/style changes directly');
  assert.equal(contentObservation.options.attributes, true);
  assert.deepEqual([...contentObservation.options.attributeFilter], ['class', 'style']);
  assert.notEqual(contentObservation.options.subtree, true);
});

test('elapsed-time ticks do not rebuild task rows or repeat static status announcements', () => {
  const browser = createBrowser('/platform/knowledge-bases/kb-1');
  const form = new FakeFormData();
  form.set('file', { name: 'quiet.pdf', size: 1024 });
  const xhr = new browser.window.XMLHttpRequest();
  xhr.open('POST', '/api/v1/knowledge-bases/kb-1/knowledge/file');
  xhr.send(form);

  const list = browser.host.shadowRoot.querySelector('.list');
  const announcer = browser.host.shadowRoot.querySelector('.announcer');
  const activeRenderCount = list.replaceChildrenCount;
  const activeAnnouncement = announcer.textContent;
  browser.runIntervals();
  assert.equal(list.replaceChildrenCount, activeRenderCount, 'active elapsed-time ticks must update in place');
  assert.equal(announcer.textContent, activeAnnouncement);

  xhr.status = 204;
  xhr.emit('load');
  const terminalRenderCount = list.replaceChildrenCount;
  const terminalAnnouncement = announcer.textContent;
  browser.runIntervals();
  assert.equal(list.replaceChildrenCount, terminalRenderCount, 'static terminal rows must not be re-announced');
  assert.equal(announcer.textContent, terminalAnnouncement, 'static terminal state must not be repeated');
});

test('upload monitor stays below upload confirmation, document detail, and Trace surfaces', () => {
  const monitorLayer = Number(
    monitorSource.match(/\.root\s*\{[^}]*z-index:\s*(\d+)/s)?.[1],
  );

  assert.ok(Number.isFinite(monitorLayer), 'monitor root z-index must be numeric');
  assert.ok(monitorLayer < 1900, 'monitor must stay below the upload confirmation modal');
  assert.match(uploadConfirmSource, /\.upload-confirm-overlay\s*\{[^}]*z-index:\s*1900\s*;/s);
  assert.match(uploadConfirmSource, /:z-index="1950"/);
  assert.match(documentDetailSource, /:zIndex="2000"/);
  assert.match(documentDetailSource, /:zIndex="2100"/);
  assert.ok(monitorLayer < 1950, 'UploadConfirm menus must stay above the monitor');
  assert.ok(monitorLayer < 2000, 'document detail must stay above the monitor');
  assert.ok(monitorLayer < 2100, 'Trace must stay above the monitor');
});

test('overlay exposes the 1.3.1 behavior version', () => {
  const browser = createBrowser('/platform/knowledge-bases/kb-1');
  assert.equal(browser.window.__weknoraUploadMonitor.version, '1.3.1');
  assert.equal(browser.host.dataset.version, '1.3.1');
});

test('nginx injection uses the v3 cache-busted 1.3.1 script URL', () => {
  assert.match(monitorConfig, /weknora-upload-monitor\.js\?v=3/);
  assert.doesNotMatch(monitorConfig, /weknora-upload-monitor\.js\?v=[12](?:["'])/);
});

test('README documents refresh interruption and only the upload API actually called by the overlay', () => {
  assert.match(monitorReadme, /页面刷新后保留最近记录.*未完成.*上传中断/);
  assert.doesNotMatch(monitorReadme, /恢复当前知识库内仍在处理的条目/);
  assert.doesNotMatch(monitorReadme, /^\s*-\s*`GET\s+/m);
  assert.match(monitorReadme, /`POST \/api\/v1\/knowledge-bases\/\{kbId\}\/knowledge\/file`/);
});
