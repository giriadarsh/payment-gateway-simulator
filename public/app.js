// Payment notification simulator UI. Vanilla JS, no build step.
// State arrives as periodic snapshots and a live event stream (Server-Sent Events).

const $ = (selector, root = document) => root.querySelector(selector);

function el(tag, props = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key === 'dataset') Object.assign(node.dataset, value);
    else if (key.startsWith('on') && typeof value === 'function') node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

const SVG_NS = 'http://www.w3.org/2000/svg';
function svgEl(tag, attrs = {}, text) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  if (text !== undefined) node.textContent = text;
  return node;
}

async function api(path, { method = 'GET', body } = {}) {
  const res = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

function toast(message, tone = 'accent') {
  const node = el('div', { class: 'toast', dataset: { tone }, role: 'status' }, message);
  $('#toasts').append(node);
  setTimeout(() => node.remove(), 4500);
}

const storage = {
  get(key) {
    try {
      return localStorage.getItem(key);
    } catch {
      return null;
    }
  },
  set(key, value) {
    try {
      localStorage.setItem(key, value);
    } catch {
      /* storage unavailable: preference is not remembered */
    }
  },
};

const fmtMs = (ms) => (ms < 1000 ? `${Math.max(0, Math.round(ms))} ms` : `${(ms / 1000).toFixed(1)} s`);
const pad = (n, width = 2) => String(n).padStart(width, '0');
const clock = (ts, withMs = true) => {
  const d = new Date(ts);
  const base = `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  return withMs ? `${base}.${pad(d.getMilliseconds(), 3)}` : base;
};
const shortTxn = (id) => (id ? `…${String(id).slice(-6)}` : '');
const fill = (text, vars) => text.replace(/\{(\w+)\}/g, (match, key) => (key in vars ? String(vars[key]) : match));

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TXN_STATES = {
  PENDING: ['Queued', 'neutral'],
  DELIVERING: ['Delivering', 'accent'],
  DELIVERED: ['Delivered', 'good'],
  RETRY_SCHEDULED: ['Retry scheduled', 'serious'],
  RATE_LIMITED: ['Rate limited', 'warning'],
  HELD_CLIENT_DOWN: ['Held: client down', 'critical'],
  FAILED: ['Dead-lettered', 'critical'],
};

const EVENT_TONES = {
  DELIVERY_SUCCEEDED: 'good',
  CLIENT_MARKED_UP: 'good',
  PARTITION_RESUMED: 'good',
  SCENARIO_FINISHED: 'good',
  DELIVERY_FAILED: 'critical',
  CLIENT_MARKED_DOWN: 'critical',
  DEAD_LETTERED: 'critical',
  SCENARIO_FAILED: 'critical',
  PARTITION_HELD: 'serious',
  RATE_LIMITED: 'warning',
  ADAPTIVE_RATE_CHANGED: 'warning',
  THROTTLED: 'accent',
  HEALTH_CHECK_REQUESTED: 'health',
  HEALTH_MONITOR_STARTED: 'health',
  HEALTH_PROBE: 'health',
  HEALTH_MONITOR_STOPPED: 'health',
  SCENARIO_STARTED: 'accent',
  SCENARIO_STEP: 'accent',
};

const BEHAVIORS = [
  { value: 'healthy', label: 'Webhook healthy', patch: { mode: 'HEALTHY' } },
  { value: 'down-503', label: 'Down: HTTP 503', patch: { mode: 'DOWN', failureStyle: 'HTTP_503' } },
  { value: 'down-timeout', label: 'Down: timeout', patch: { mode: 'DOWN', failureStyle: 'TIMEOUT' } },
  { value: 'down-reset', label: 'Down: connection reset', patch: { mode: 'DOWN', failureStyle: 'CONNECTION_RESET' } },
  { value: 'flaky', label: 'Flaky: next 3 requests fail', patch: { mode: 'FLAKY', failNext: 3, failureStyle: 'HTTP_503' } },
  { value: 'rate-limited', label: 'Rate limited: 2 req/s', patch: { mode: 'RATE_LIMITED', rateLimitPerSec: 2, retryAfterSec: 1 } },
];

const FAILURE_LABELS = { HTTP_503: 'HTTP 503', HTTP_500: 'HTTP 500', TIMEOUT: 'timeout', CONNECTION_RESET: 'connection reset' };

const SETTING_FIELDS = [
  { key: 'maxAttempts', label: 'Max attempts', min: 1, max: 10, step: 1 },
  { key: 'baseBackoffMs', label: 'Base backoff (ms)', min: 100, max: 10000, step: 100 },
  { key: 'maxBackoffMs', label: 'Max backoff (ms)', min: 500, max: 60000, step: 500 },
  { key: 'requestTimeoutMs', label: 'Webhook timeout (ms)', min: 200, max: 10000, step: 100 },
  { key: 'healthCheckIntervalMs', label: 'Probe interval (ms)', min: 500, max: 30000, step: 500 },
  { key: 'healthyThreshold', label: 'Healthy probes needed', min: 1, max: 5, step: 1 },
  {
    key: 'rateLimitStrategy',
    label: 'Rate-limit strategy',
    type: 'select',
    options: [
      { value: 'reactive', label: 'Reactive: honour Retry-After' },
      { value: 'token-bucket', label: 'Token bucket: contracted limit' },
      { value: 'adaptive', label: 'Adaptive: AIMD' },
    ],
  },
];

const CHART_WINDOW = 60;
const CHART_SERIES = [
  { key: 'ok', label: 'Delivered', sub: '2xx', tone: 'good', glyph: 'check' },
  { key: 'rl', label: 'Rate limited', sub: 'HTTP 429', tone: 'warning', glyph: 'pause' },
  { key: 'fail', label: 'Failed', sub: '5xx, timeout, reset', tone: 'critical', glyph: 'cross' },
];
const CHART_KIND = { DELIVERY_SUCCEEDED: 'ok', RATE_LIMITED: 'rl', DELIVERY_FAILED: 'fail' };

const MAX_EVENTS = 1500;
const MAX_LOG_ROWS = 300;
const MAX_PACKETS = 60;
const reducedMotion = matchMedia('(prefers-reduced-motion: reduce)');

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const state = {
  meta: null,
  snapshot: null,
  selected: null,
  params: {},
  scope: 'all',
  showDebug: true,
  events: [],
  holds: new Map(), // partition -> { hold, ts }
  transient: new Map(), // partition -> { tone, label, detail, at, ttl }
  run: null,
  runTs: 0,
  chart: new Map(), // second -> { ok: {clientId: n}, rl: {...}, fail: {...} }
  chartAsTable: false,
  hoverIndex: null,
  settingsDirty: false,
  packets: 0,
  lanes: new Map(),
};

const clientById = (id) => state.meta.clients.find((c) => c.id === id);
const partitionOf = (event) => event.partition ?? clientById(event.clientId)?.partition ?? null;
const currentScenario = () => state.meta.scenarios.find((s) => s.id === state.selected) ?? state.meta.scenarios[0];
const settings = () => state.snapshot?.settings ?? state.meta.defaultSettings;

// ---------------------------------------------------------------------------
// Theme
// ---------------------------------------------------------------------------

function effectiveTheme() {
  return document.documentElement.dataset.theme ?? (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
}

function applyTheme(theme) {
  if (theme) document.documentElement.dataset.theme = theme;
  $('#theme-toggle').textContent = effectiveTheme() === 'dark' ? 'Light theme' : 'Dark theme';
}

// ---------------------------------------------------------------------------
// Sidebar: scenarios, parameters, manual controls, settings
// ---------------------------------------------------------------------------

function buildScenarioList() {
  $('#scenario-list').replaceChildren(
    ...state.meta.scenarios.map((scenario) =>
      el(
        'button',
        {
          type: 'button',
          class: 'scenario-card',
          role: 'radio',
          'aria-checked': String(scenario.id === state.selected),
          dataset: { id: scenario.id },
          onclick: () => selectScenario(scenario.id),
        },
        el('span', { class: 'scenario-num' }, scenario.number),
        el('span', {}, el('span', { class: 'scenario-title' }, scenario.title), el('span', { class: 'scenario-summary' }, scenario.summary)),
      ),
    ),
  );
}

function selectScenario(id) {
  state.selected = id;
  storage.set('pns.scenario', id);
  for (const card of document.querySelectorAll('.scenario-card')) card.setAttribute('aria-checked', String(card.dataset.id === id));
  renderParams();
  renderNarration(true);
}

function renderParams() {
  const scenario = currentScenario();
  const values = (state.params[scenario.id] ??= Object.fromEntries(scenario.params.map((p) => [p.key, p.default])));
  const box = $('#scenario-params');
  box.replaceChildren();
  for (const spec of scenario.params) {
    let control;
    if (spec.type === 'number') {
      control = el('input', { type: 'number', name: spec.key, min: spec.min, max: spec.max, step: 1, value: values[spec.key] });
    } else {
      const options =
        spec.type === 'client'
          ? state.meta.clients.map((c) => ({ value: c.id, label: `${c.name} (P${c.partition})` }))
          : spec.options;
      control = el('select', { name: spec.key }, options.map((o) => el('option', { value: o.value, selected: o.value === values[spec.key] }, o.label)));
    }
    control.addEventListener('input', () => {
      values[spec.key] = spec.type === 'number' ? Number(control.value) : control.value;
      renderNarration(true);
    });
    const wide = spec.type === 'select' || spec.label.length > 24;
    box.append(el('label', { class: `field${wide ? ' wide' : ''}` }, spec.label, control));
  }
  syncRunButton();
}

function syncRunButton() {
  const running = state.run?.status === 'running';
  $('#run').textContent = running && state.run.id === state.selected ? 'Restart scenario' : `Run scenario ${currentScenario().number}`;
  $('#cancel').hidden = !running;
}

function buildClientControls() {
  const box = $('#client-controls');
  box.replaceChildren();
  for (const client of state.meta.clients) {
    const select = el(
      'select',
      { 'aria-label': `${client.name} webhook behaviour`, dataset: { client: client.id } },
      BEHAVIORS.map((b) => el('option', { value: b.value }, b.label)),
    );
    select.addEventListener('change', async () => {
      const behavior = BEHAVIORS.find((b) => b.value === select.value);
      try {
        await api(`/api/clients/${client.id}/behavior`, { method: 'PUT', body: behavior.patch });
      } catch (err) {
        toast(err.message, 'critical');
      }
    });

    const statusButtons = ['UP', 'DOWN'].map((status) =>
      el(
        'button',
        {
          type: 'button',
          'aria-pressed': 'false',
          dataset: { status },
          onclick: async () => {
            try {
              await api(`/api/clients/${client.id}/status`, { method: 'PUT', body: { status } });
            } catch (err) {
              toast(err.message, 'critical');
            }
          },
        },
        status,
      ),
    );

    const send = el(
      'button',
      {
        type: 'button',
        class: 'btn small',
        onclick: async () => {
          try {
            await api('/api/transactions', { method: 'POST', body: { clientId: client.id, count: 5, intervalMs: 250 } });
          } catch (err) {
            toast(err.message, 'critical');
          }
        },
      },
      'Send 5',
    );

    box.append(
      el(
        'div',
        { class: 'client-control' },
        el('div', { class: 'client-control-head' }, el('strong', {}, client.name), el('span', { class: 'mono secondary' }, `${client.id} · P${client.partition}`)),
        el(
          'div',
          { class: 'client-control-row' },
          select,
          el('div', { class: 'seg', role: 'group', 'aria-label': `Redis status of ${client.name}` }, statusButtons),
          send,
        ),
      ),
    );
  }
}

function behaviorValue(behavior) {
  if (!behavior) return 'healthy';
  if (behavior.mode === 'DOWN') return { TIMEOUT: 'down-timeout', CONNECTION_RESET: 'down-reset' }[behavior.failureStyle] ?? 'down-503';
  if (behavior.mode === 'FLAKY') return 'flaky';
  if (behavior.mode === 'RATE_LIMITED') return 'rate-limited';
  return 'healthy';
}

function syncClientControls() {
  for (const client of state.snapshot.clients) {
    const select = $(`select[data-client="${client.id}"]`);
    if (select && document.activeElement !== select) select.value = behaviorValue(client.mock?.behavior);
    const group = select?.closest('.client-control');
    for (const button of group?.querySelectorAll('.seg button') ?? []) {
      button.setAttribute('aria-pressed', String(button.dataset.status === client.status));
    }
  }
}

function buildSettingsForm() {
  const form = $('#settings-form');
  form.replaceChildren();
  for (const field of SETTING_FIELDS) {
    const control =
      field.type === 'select'
        ? el('select', { name: field.key }, field.options.map((o) => el('option', { value: o.value }, o.label)))
        : el('input', { type: 'number', name: field.key, min: field.min, max: field.max, step: field.step });
    control.addEventListener('input', () => {
      state.settingsDirty = true;
    });
    form.append(el('label', { class: `field${field.type === 'select' ? ' wide' : ''}` }, field.label, control));
  }
  const restore = el('button', { type: 'button', class: 'btn' }, 'Restore defaults');
  restore.addEventListener('click', async () => {
    try {
      await api('/api/settings/reset', { method: 'POST' });
      state.settingsDirty = false;
      toast('Default delivery settings restored');
    } catch (err) {
      toast(err.message, 'critical');
    }
  });
  form.append(el('div', { class: 'actions' }, el('button', { type: 'submit', class: 'btn primary' }, 'Apply'), restore));
  form.addEventListener('submit', async (event) => {
    event.preventDefault();
    const body = {};
    for (const field of SETTING_FIELDS) {
      const value = form.elements[field.key].value;
      body[field.key] = field.type === 'select' ? value : Number(value);
    }
    try {
      await api('/api/settings', { method: 'PUT', body });
      state.settingsDirty = false;
      toast('Delivery settings applied');
    } catch (err) {
      toast(err.message, 'critical');
    }
  });
}

function syncSettingsForm() {
  const form = $('#settings-form');
  if (state.settingsDirty || form.contains(document.activeElement)) return;
  for (const field of SETTING_FIELDS) form.elements[field.key].value = settings()[field.key];
}

// ---------------------------------------------------------------------------
// Narration (scenario steps)
// ---------------------------------------------------------------------------

function backoffPlan(s) {
  const delays = [];
  for (let attempt = 1; attempt < s.maxAttempts && delays.length < 4; attempt += 1) {
    const ms = Math.min(s.maxBackoffMs, s.baseBackoffMs * 2 ** (attempt - 1));
    delays.push(ms < 1000 ? `${ms} ms` : `${ms / 1000} s`);
  }
  return `${delays.join(', ')}${s.maxAttempts > 5 ? ' …' : ''}`;
}

function previewSteps(scenario) {
  const params = state.params[scenario.id] ?? {};
  const s = settings();
  const vars = {
    ...params,
    partition: clientById(params.clientId)?.partition ?? '',
    otherClient: state.meta.clients.find((c) => c.id !== params.clientId)?.name,
    maxAttempts: s.maxAttempts,
    healthyThreshold: s.healthyThreshold,
    backoffPlan: backoffPlan(s),
    failureStyle: FAILURE_LABELS[params.failureStyle] ?? params.failureStyle,
  };
  vars.strategyText = params.strategy ? fill(scenario.strategyText[params.strategy], vars) : '';
  return scenario.steps.map((text) => fill(text, vars));
}

function summaryLine(summary) {
  if (!summary) return '';
  const parts = [`${summary.delivered}/${summary.total} delivered in ${(summary.durationMs / 1000).toFixed(1)} s`, `${summary.attempts} webhook calls`];
  if (summary.http429 !== undefined) parts.push(`${summary.http429} × HTTP 429`);
  if (summary.pacingWaits) parts.push(`${summary.pacingWaits} pacing waits`);
  if (summary.strategy) parts.push(`strategy: ${summary.strategy}`);
  return parts.join(' · ');
}

let narrationKey = '';
function renderNarration(force = false) {
  const run = state.run;
  const selected = currentScenario();
  const active = run && (run.status === 'running' || run.id === selected.id) ? run : null;
  const scenario = active ? state.meta.scenarios.find((s) => s.id === active.id) ?? selected : selected;
  const key = JSON.stringify([scenario.id, active?.status, active?.stepIndex, active?.summary, active?.error, force ? Math.random() : 0]);
  if (key === narrationKey) return;
  narrationKey = key;

  const statusChip = active
    ? {
        running: ['Running', 'accent'],
        finished: ['Finished', 'good'],
        failed: ['Failed', 'critical'],
        cancelled: ['Cancelled', 'neutral'],
      }[active.status]
    : ['Not started', 'neutral'];

  const steps = active?.steps ?? previewSteps(scenario);
  const list = el(
    'ol',
    { class: 'steps' },
    steps.map((text, index) => {
      let stepState = 'pending';
      if (active?.status === 'finished') stepState = 'done';
      else if (active && index < active.stepIndex) stepState = 'done';
      else if (active && index === active.stepIndex) stepState = 'active';
      return el('li', { dataset: { state: stepState } }, el('span', {}, text));
    }),
  );

  const box = $('#narration');
  const parts = [
    el(
      'div',
      { class: 'narration-head' },
      el(
        'div',
        { class: 'narration-title' },
        el('h2', {}, `Scenario ${scenario.number}: ${scenario.title}`),
        el('span', { class: 'chip', dataset: { tone: statusChip[1] } }, statusChip[0]),
      ),
      el('span', { class: 'small secondary', id: 'elapsed' }),
    ),
    el('p', {}, scenario.summary),
    list,
    active?.status === 'finished' ? el('div', { class: 'result' }, el('strong', {}, 'Result: '), summaryLine(active.summary)) : null,
    active?.status === 'failed' ? el('div', { class: 'result' }, el('strong', {}, 'Stopped: '), active.error) : null,
    !active ? el('p', { class: 'small' }, 'Adjust the parameters on the left and press Run. Every step below is performed by the real services.') : null,
  ];
  // replaceChildren() would render null as the text "null".
  box.replaceChildren(...parts.filter(Boolean));
  updateElapsed();
  syncRunButton();
}

function updateElapsed() {
  const node = $('#elapsed');
  const run = state.run;
  if (!node || !run || !(run.status === 'running' || run.id === state.selected)) return;
  const end = run.status === 'running' ? Date.now() : run.finishedAt ?? Date.now();
  node.textContent = `${((end - run.startedAt) / 1000).toFixed(1)} s`;
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

function buildLanes() {
  const lanes = $('#lanes');
  lanes.replaceChildren();
  const clients = [...state.meta.clients].sort((a, b) => a.partition - b.partition);
  for (const client of clients) {
    const p = client.partition;
    const refs = {
      queue: el('div', { class: 'queue', role: 'img', 'aria-label': 'Messages waiting in this partition' }),
      offsets: el('div', { class: 'offsets mono' }),
      workerState: el('div', { class: 'worker-state' }, 'Idle'),
      workerDetail: el('div', { class: 'worker-detail' }),
      countdown: el('div', { class: 'countdown' }, el('span')),
      chips: el('div', { class: 'chips' }),
      stats: el('div', { class: 'client-stats' }),
    };
    refs.worker = el('div', { class: 'cell worker', dataset: { anchor: `worker-${p}` } }, refs.workerState, refs.workerDetail, refs.countdown);
    lanes.append(
      el(
        'div',
        { class: 'lane', dataset: { client: client.id } },
        el('div', { class: 'link' }),
        el(
          'div',
          { class: 'cell partition', dataset: { anchor: `partition-${p}` } },
          el('div', { class: 'cell-head' }, el('span', { class: 'pill' }, `P${p}`), el('span', { class: 'small secondary' }, `key = ${client.id}`)),
          refs.queue,
          refs.offsets,
        ),
        el('div', { class: 'link' }),
        refs.worker,
        el('div', { class: 'link' }),
        el(
          'div',
          { class: 'cell client', dataset: { anchor: `client-${client.id}` } },
          el('div', { class: 'client-head' }, el('strong', {}, client.name), el('span', { class: 'mono secondary' }, client.id)),
          refs.chips,
          refs.stats,
        ),
      ),
    );
    state.lanes.set(p, refs);
  }
}

function setText(node, text) {
  if (node.textContent !== text) node.textContent = text;
}

/** Rebuilds a node's children only when `key` changes, to keep the DOM calm. */
function setChildren(node, key, build) {
  if (node._key === key) return;
  node._key = key;
  node.replaceChildren(...build());
}

function currentHold(p) {
  const hold = state.holds.get(p)?.hold;
  if (!hold) return null;
  if (hold.until && hold.until < Date.now() - 1500) return null;
  return hold;
}

function holdView(hold) {
  const remaining = hold.until ? fmtMs(hold.until - Date.now()) : '';
  switch (hold.reason) {
    case 'CLIENT_DOWN':
      return { tone: 'critical', label: 'Holding: client DOWN', detail: `${shortTxn(hold.txnId)} unacknowledged, waiting for status UP`, indefinite: true };
    case 'BACKOFF':
      return { tone: 'serious', label: `Backoff before attempt ${hold.attempt ?? ''}`, detail: `retry in ${remaining}` };
    case 'RATE_LIMITED':
      return { tone: 'warning', label: 'Cooling down after 429', detail: `retry in ${remaining} (Retry-After)` };
    case 'PACING':
      return { tone: 'accent', label: 'Pacing', detail: `next call in ${remaining}${hold.rate ? ` at ${hold.rate} req/s` : ''}` };
    default:
      return { tone: 'critical', label: 'Error, retrying', detail: remaining ? `in ${remaining}` : '' };
  }
}

function workerView(p) {
  const hold = currentHold(p);
  if (hold) return { ...holdView(hold), hold };
  const transient = state.transient.get(p);
  if (transient && Date.now() - transient.at < transient.ttl) return transient;
  return { tone: 'neutral', label: 'Idle', detail: 'waiting for messages' };
}

function renderWorker(p) {
  const refs = state.lanes.get(p);
  if (!refs) return;
  const view = workerView(p);
  refs.worker.dataset.tone = view.tone;
  setText(refs.workerState, view.label);
  setText(refs.workerDetail, view.detail ?? '');
  const bar = refs.countdown.firstChild;
  refs.countdown.classList.toggle('indefinite', Boolean(view.indefinite));
  if (view.hold?.until && view.hold.since) {
    const total = Math.max(1, view.hold.until - view.hold.since);
    const left = Math.max(0, view.hold.until - Date.now());
    bar.style.width = `${(left / total) * 100}%`;
  } else {
    bar.style.width = view.indefinite ? '' : '0';
  }
}

function mockChip(behavior) {
  if (!behavior) return { tone: 'neutral', text: 'webhook unknown' };
  switch (behavior.mode) {
    case 'DOWN': {
      const back = behavior.recoverInMs ? `, back in ${Math.ceil(behavior.recoverInMs / 1000)} s` : '';
      return { tone: 'critical', text: `webhook down: ${FAILURE_LABELS[behavior.failureStyle] ?? behavior.failureStyle}${back}` };
    }
    case 'FLAKY':
      return { tone: 'serious', text: `flaky: ${behavior.failNext} more fail` };
    case 'RATE_LIMITED':
      return { tone: 'warning', text: `limit ${behavior.rateLimitPerSec} req/s` };
    default:
      return { tone: 'good', text: 'webhook healthy' };
  }
}

function renderLanes() {
  const snap = state.snapshot;
  for (const part of snap.partitions) {
    const refs = state.lanes.get(part.partition);
    if (!refs) continue;
    const hold = currentHold(part.partition);
    const headTone = hold ? holdView(hold).tone : null;
    const shown = Math.min(part.lag, 18);
    setChildren(refs.queue, `${part.lag}|${headTone}`, () => {
      if (!part.lag) return [el('span', { class: 'empty' }, 'no backlog')];
      const squares = Array.from({ length: shown }, (_, i) =>
        el('span', { class: `msg${i === 0 && headTone ? ' head' : ''}`, dataset: i === 0 && headTone ? { tone: headTone } : undefined }),
      );
      if (part.lag > shown) squares.push(el('span', { class: 'more' }, `+${part.lag - shown}`));
      return squares;
    });
    refs.queue.setAttribute('aria-label', `${part.lag} message(s) not yet acknowledged`);
    refs.offsets.textContent = `committed ${part.committed < 0 ? '–' : part.committed} · end ${part.high} · lag ${part.lag}`;
    renderWorker(part.partition);
  }

  for (const client of snap.clients) {
    const refs = state.lanes.get(client.partition);
    if (!refs) continue;
    const chips = [
      mockChip(client.mock?.behavior),
      { tone: client.status === 'DOWN' ? 'critical' : 'good', text: `status ${client.status}` },
    ];
    if (client.throttleMs > 0) chips.push({ tone: 'warning', text: `throttled ${fmtMs(client.throttleMs)}` });
    if (client.healthCheckPending) chips.push({ tone: 'health', text: 'health check pending' });
    setChildren(refs.chips, JSON.stringify(chips), () => chips.map((c) => el('span', { class: 'chip', dataset: { tone: c.tone } }, c.text)));

    const s = client.stats;
    const parts = [`${s.delivered}/${s.produced} delivered`];
    if (s.failures) parts.push(`${s.failures} failed attempts`);
    if (s.rateLimited) parts.push(`${s.rateLimited} × 429`);
    if (s.markedDown) parts.push(`marked DOWN ${s.markedDown}×`);
    if (snap.settings.rateLimitStrategy === 'adaptive' && client.adaptiveRate) parts.push(`adaptive ${client.adaptiveRate} req/s`);
    refs.stats.textContent = parts.join(' · ');
  }

  const produced = snap.clients.reduce((sum, c) => sum + c.stats.produced, 0);
  $('#produced-count').textContent = produced.toLocaleString();
}

function renderRedis() {
  const snap = state.snapshot;
  const chip = (tone, text) => el('span', { class: 'chip', dataset: { tone } }, text);
  const none = () => el('span', { class: 'secondary' }, '(none)');
  const throttled = snap.clients.filter((c) => c.throttleMs > 0);
  const pending = snap.clients.filter((c) => c.healthCheckPending);
  const holds = snap.partitions.filter((p) => p.hold);
  const s = snap.settings;
  const rows = [
    ['client:{id}:status', snap.clients.map((c) => chip(c.status === 'DOWN' ? 'critical' : 'good', `${c.id} ${c.status}`))],
    ['client:{id}:throttle', throttled.length ? throttled.map((c) => chip('warning', `${c.id} ${fmtMs(c.throttleMs)}`)) : [none()]],
    ['client:{id}:healthcheck', pending.length ? pending.map((c) => chip('health', `${c.id} pending`)) : [none()]],
    [
      'stream:client-health-checks',
      [el('span', {}, `${snap.healthStream.length} event(s), ${snap.healthStream.pending} pending acknowledgement`)],
    ],
    ['txn:{id}', [el('span', {}, `${snap.txnCount} payload(s) stored`)]],
    ['consumer:holds', holds.length ? holds.map((p) => chip(holdView(p.hold).tone, `P${p.partition} ${p.hold.reason}`)) : [none()]],
    [
      'config:delivery',
      [el('span', {}, `${s.maxAttempts} attempts · backoff ${s.baseBackoffMs} ms → ${s.maxBackoffMs} ms · ${s.rateLimitStrategy}`)],
    ],
  ];
  const key = JSON.stringify(rows.map(([k, nodes]) => [k, nodes.map((n) => n.textContent)]));
  setChildren($('#redis-kv'), key, () => rows.flatMap(([k, nodes]) => [el('dt', {}, k), el('dd', {}, nodes)]));
}

function renderMonitors() {
  const monitors = state.snapshot.monitors;
  const key = JSON.stringify(monitors.map((m) => [m.clientId, m.probes, m.consecutiveOk, m.lastResult]));
  setChildren($('#monitors'), key, () => {
    if (!monitors.length) {
      return [el('div', { class: 'monitor' }, 'Idle: waiting for "check client status" events on ', el('span', { class: 'mono' }, 'stream:client-health-checks'))];
    }
    return monitors.map((m) =>
      el(
        'div',
        { class: 'monitor' },
        el('strong', {}, clientById(m.clientId)?.name ?? m.clientId),
        ` · probe #${m.probes} → ${m.lastResult}`,
        el('div', { class: 'secondary' }, `${m.consecutiveOk}/${m.required} healthy in a row needed to mark UP`),
        el(
          'div',
          { class: 'probe-dots', 'aria-hidden': 'true' },
          Array.from({ length: m.required }, (_, i) => el('i', { class: i < m.consecutiveOk ? 'ok' : '' })),
        ),
      ),
    );
  });
}

function renderTiles() {
  const clients = state.snapshot.clients;
  const sum = (field) => clients.reduce((total, c) => total + c.stats[field], 0);
  const lag = state.snapshot.partitions.reduce((total, p) => total + p.lag, 0);
  const tiles = [
    ['Produced', sum('produced')],
    ['Delivered', sum('delivered')],
    ['Waiting in Kafka', lag],
    ['Failed attempts', sum('failures')],
    ['HTTP 429 responses', sum('rateLimited')],
    ['Times marked DOWN', sum('markedDown')],
  ];
  setChildren($('#tiles'), JSON.stringify(tiles), () =>
    tiles.map(([label, value]) => el('div', { class: 'panel tile' }, el('div', { class: 'tile-label' }, label), el('div', { class: 'tile-value' }, value.toLocaleString()))),
  );
}

function renderTxns() {
  const txns = state.snapshot.txns.filter((t) => state.scope === 'all' || t.clientId === state.scope);
  const key = JSON.stringify([state.scope, txns.map((t) => [t.txnId, t.state, t.totalAttempts, t.lastResult, t.deliveredAt])]);
  setChildren($('#txn-table'), key, () => {
    const head = el('thead', {}, el('tr', {}, el('th', {}, 'Transaction'), el('th', {}, 'Client'), el('th', {}, 'State'), el('th', { class: 'num' }, 'Calls'), el('th', {}, 'Last result'), el('th', { class: 'num' }, 'Delivered after')));
    const rows = txns.length
      ? txns.map((t) => {
          const [label, tone] = TXN_STATES[t.state] ?? [t.state, 'neutral'];
          const latency = t.deliveredAt && t.producedAt ? fmtMs(t.deliveredAt - t.producedAt) : '–';
          return el(
            'tr',
            {},
            el('td', { class: 'mono', title: `P${t.partition} offset ${t.offset}` }, t.txnId),
            el('td', {}, t.clientId),
            el('td', {}, el('span', { class: 'chip', dataset: { tone } }, label)),
            el('td', { class: 'num' }, t.totalAttempts),
            el('td', {}, t.lastResult ?? '–'),
            el('td', { class: 'num' }, latency),
          );
        })
      : [el('tr', { class: 'empty-row' }, el('td', { colspan: 6 }, 'No transactions yet. Run a scenario or send some traffic.'))];
    return [el('table', {}, head, el('tbody', {}, rows))];
  });
}

// ---------------------------------------------------------------------------
// Snapshots and events
// ---------------------------------------------------------------------------

function applySnapshot(snapshot) {
  state.snapshot = snapshot;
  for (const part of snapshot.partitions) {
    const entry = state.holds.get(part.partition);
    if (!entry || snapshot.ts > entry.ts + 300) state.holds.set(part.partition, { hold: part.hold, ts: snapshot.ts });
  }
  if (snapshot.scenario !== undefined && snapshot.ts > state.runTs + 300) {
    state.run = snapshot.scenario;
    state.runTs = snapshot.ts;
  }
  renderTiles();
  renderLanes();
  renderRedis();
  renderMonitors();
  renderTxns();
  syncClientControls();
  syncSettingsForm();
  renderNarration();
  $('#pipeline-sub').textContent = snapshot.services.kafka
    ? 'Messages flow left to right; each client owns one partition, so its backlog never delays another client.'
    : `Kafka offsets unavailable: ${snapshot.services.kafkaError}`;
}

function setTransient(p, tone, label, detail, ttl = 1800) {
  state.transient.set(p, { tone, label, detail, at: Date.now(), ttl });
}

function clearHold(p, ts) {
  const entry = state.holds.get(p);
  if (entry?.hold && ts >= entry.ts) state.holds.set(p, { hold: null, ts });
}

function trackEvent(e) {
  const p = partitionOf(e);
  switch (e.type) {
    case 'MESSAGE_CONSUMED':
      clearHold(p, e.ts);
      setTransient(p, 'accent', 'Processing', `${shortTxn(e.txnId)} at offset ${e.offset}`, 4000);
      break;
    case 'STATUS_CHECKED':
      clearHold(p, e.ts);
      if (e.status === 'DOWN') setTransient(p, 'critical', 'Status DOWN in Redis', 'webhook not called');
      break;
    case 'DELIVERY_ATTEMPT':
      clearHold(p, e.ts);
      setTransient(p, 'accent', `Calling webhook, attempt ${e.attempt}/${e.maxAttempts}`, shortTxn(e.txnId), 15_000);
      break;
    case 'DELIVERY_SUCCEEDED':
      setTransient(p, 'good', 'Delivered', `HTTP ${e.statusCode} in ${fmtMs(e.latencyMs)}, offset committed`);
      break;
    case 'DELIVERY_FAILED':
      setTransient(p, 'critical', `Attempt ${e.attempt} failed`, e.result, 3000);
      break;
    case 'RATE_LIMITED':
      setTransient(p, 'warning', 'HTTP 429 received', `cool down ${fmtMs(e.retryAfterMs)}`);
      break;
    case 'DUPLICATE_SKIPPED':
      setTransient(p, 'neutral', 'Duplicate skipped', shortTxn(e.txnId));
      break;
    case 'PARTITION_HELD':
      state.holds.set(p, { hold: e, ts: e.ts });
      break;
    case 'PARTITION_RESUMED':
      state.holds.set(p, { hold: null, ts: e.ts });
      setTransient(p, 'good', 'Resumed', 're-evaluating the held message', 1500);
      break;
    case 'THROTTLED':
      state.holds.set(p, { hold: { reason: 'PACING', since: e.ts, until: e.ts + e.waitMs, rate: e.rate, txnId: e.txnId }, ts: e.ts });
      break;
    case 'SCENARIO_STARTED': {
      const meta = state.meta.scenarios.find((s) => s.id === e.scenarioId);
      state.run = { id: e.scenarioId, number: meta?.number, title: meta?.title, steps: e.steps, params: e.params, stepIndex: -1, status: 'running', startedAt: e.ts };
      state.runTs = e.ts;
      break;
    }
    case 'SCENARIO_STEP':
      if (state.run?.id === e.scenarioId) {
        state.run = { ...state.run, stepIndex: e.index };
        state.runTs = e.ts;
      }
      break;
    case 'SCENARIO_FINISHED':
      if (state.run?.id === e.scenarioId) {
        state.run = { ...state.run, status: 'finished', summary: e.summary, finishedAt: e.ts };
        state.runTs = e.ts;
      }
      break;
    case 'SCENARIO_FAILED':
      if (state.run?.id === e.scenarioId) {
        state.run = { ...state.run, status: 'failed', error: e.error, finishedAt: e.ts };
        state.runTs = e.ts;
      }
      break;
    default:
  }
  return p;
}

function recordChart(e) {
  const kind = CHART_KIND[e.type];
  if (!kind || !e.clientId) return;
  const second = Math.floor(e.ts / 1000);
  let bucket = state.chart.get(second);
  if (!bucket) state.chart.set(second, (bucket = { ok: {}, rl: {}, fail: {} }));
  bucket[kind][e.clientId] = (bucket[kind][e.clientId] ?? 0) + 1;
}

function ingest(e, { live }) {
  state.events.push(e);
  if (state.events.length > MAX_EVENTS) state.events.splice(0, state.events.length - MAX_EVENTS);
  recordChart(e);
  const p = trackEvent(e);
  if (!live) return;
  prependLog(e, true);
  animate(e, p);
  if (p !== null && state.snapshot) renderWorker(p);
  if (e.type.startsWith('SCENARIO_')) renderNarration();
}

// ---------------------------------------------------------------------------
// Event log
// ---------------------------------------------------------------------------

const inScope = (e) =>
  (state.scope === 'all' || e.clientId === state.scope || (!e.clientId && e.service === 'dashboard')) &&
  (state.showDebug || e.level !== 'debug');

function eventTone(e) {
  if (e.type === 'STATUS_CHECKED') return e.status === 'DOWN' ? 'critical' : undefined;
  if (e.type === 'HEALTH_PROBE' && e.ok) return 'good';
  return EVENT_TONES[e.type];
}

function logRow(e, fresh) {
  const tone = eventTone(e);
  return el(
    'li',
    { class: fresh ? 'fresh' : undefined, dataset: tone ? { tone } : undefined },
    el('time', { datetime: new Date(e.ts).toISOString() }, clock(e.ts)),
    el('span', { class: 'svc' }, e.service),
    el('div', {}, el('span', { class: 'etype' }, e.type), el('span', { class: 'msg' }, e.msg ?? '')),
  );
}

function prependLog(e, fresh) {
  if (!inScope(e)) return;
  const log = $('#log');
  log.prepend(logRow(e, fresh));
  while (log.children.length > MAX_LOG_ROWS) log.lastChild.remove();
  $('#log-count').textContent = `${log.children.length} shown`;
}

function renderLog() {
  const rows = [];
  for (let i = state.events.length - 1; i >= 0 && rows.length < MAX_LOG_ROWS; i -= 1) {
    if (inScope(state.events[i])) rows.push(logRow(state.events[i], false));
  }
  $('#log').replaceChildren(...rows);
  $('#log-count').textContent = `${rows.length} shown`;
}

function buildScope() {
  const box = $('#scope');
  const options = [{ id: 'all', name: 'All clients' }, ...state.meta.clients];
  const buttons = options.map((option) =>
    el(
      'button',
      {
        type: 'button',
        class: 'chip-toggle',
        'aria-pressed': String(state.scope === option.id),
        onclick: () => {
          state.scope = option.id;
          for (const b of buttons) b.setAttribute('aria-pressed', String(b === buttons[options.indexOf(option)]));
          renderLog();
          renderChart();
          if (state.snapshot) renderTxns();
        },
      },
      option.name,
    ),
  );
  const debug = el('input', { type: 'checkbox', checked: state.showDebug });
  debug.addEventListener('change', () => {
    state.showDebug = debug.checked;
    renderLog();
  });
  box.replaceChildren(
    el('span', { class: 'scope-label' }, 'Focus'),
    ...buttons,
    el('label', { class: 'check' }, debug, 'Show every step (Redis reads, offset commits, client side)'),
  );
}

// ---------------------------------------------------------------------------
// Packet animation
// ---------------------------------------------------------------------------

function flightsFor(e, p) {
  const worker = `worker-${p}`;
  const partition = `partition-${p}`;
  const client = `client-${e.clientId}`;
  switch (e.type) {
    case 'MESSAGE_PRODUCED':
      return [['producer', partition, 'flow']];
    case 'MESSAGE_CONSUMED':
      return [[partition, worker, 'flow']];
    case 'STATUS_CHECKED':
      return [[worker, 'redis', e.status === 'DOWN' ? 'critical' : 'redis', true]];
    case 'PAYLOAD_FETCHED':
      return [['redis', worker, 'redis', true]];
    case 'DELIVERY_ATTEMPT':
      return [[worker, client, 'flow']];
    case 'DELIVERY_SUCCEEDED':
      return [[client, worker, 'good']];
    case 'DELIVERY_FAILED':
      return [[client, worker, 'critical']];
    case 'RATE_LIMITED':
      return [[client, worker, 'warning']];
    case 'OFFSET_COMMITTED':
      return [[worker, partition, 'ack', true]];
    case 'CLIENT_MARKED_DOWN':
      return [[worker, 'redis', 'critical']];
    case 'HEALTH_CHECK_REQUESTED':
      return p === null ? [] : [[worker, 'redis', 'health']];
    case 'HEALTH_MONITOR_STARTED':
      return [['redis', 'checker', 'health']];
    case 'HEALTH_PROBE':
      return [['checker', client, e.ok ? 'good' : 'health', true]];
    case 'CLIENT_MARKED_UP':
      return [['checker', 'redis', 'good']];
    case 'PARTITION_RESUMED':
      return e.reason === 'CLIENT_DOWN' ? [['redis', worker, 'good']] : [];
    case 'TXN_STORED':
      return [['producer', 'redis', 'redis', true]];
    default:
      return [];
  }
}

const anchor = (name) => document.querySelector(`[data-anchor="${name}"]`);

function endpoints(from, to, box) {
  const a = from.getBoundingClientRect();
  const b = to.getBoundingClientRect();
  const ac = { x: a.left + a.width / 2, y: a.top + a.height / 2 };
  const bc = { x: b.left + b.width / 2, y: b.top + b.height / 2 };
  const dx = bc.x - ac.x;
  const dy = bc.y - ac.y;
  let start;
  let end;
  if (Math.abs(dx) > Math.abs(dy) && Math.abs(dx) > (a.width + b.width) / 2) {
    start = { x: dx > 0 ? a.right : a.left, y: ac.y };
    end = { x: dx > 0 ? b.left : b.right, y: bc.y };
  } else {
    start = { x: ac.x, y: dy > 0 ? a.bottom : a.top };
    end = { x: bc.x, y: dy > 0 ? b.top : b.bottom };
  }
  return [
    { x: start.x - box.left, y: start.y - box.top },
    { x: end.x - box.left, y: end.y - box.top },
  ];
}

function pulse(node, kind) {
  node.dataset.packet = kind;
  node.classList.add('pulse');
  clearTimeout(node._pulse);
  node._pulse = setTimeout(() => node.classList.remove('pulse'), 450);
}

function fly(fromName, toName, kind, small) {
  const from = anchor(fromName);
  const to = anchor(toName);
  if (!from || !to) return;
  if (reducedMotion.matches || state.packets >= MAX_PACKETS) {
    pulse(to, kind);
    return;
  }
  const layer = $('#packet-layer');
  const [a, b] = endpoints(from, to, layer.getBoundingClientRect());
  const dot = el('div', { class: `packet${small ? ' small' : ''}`, dataset: { packet: kind } });
  layer.append(dot);
  state.packets += 1;
  const distance = Math.hypot(b.x - a.x, b.y - a.y);
  const animation = dot.animate(
    [
      { transform: `translate(${a.x}px, ${a.y}px)`, opacity: 0 },
      { opacity: 1, offset: 0.15 },
      { transform: `translate(${b.x}px, ${b.y}px)`, opacity: 1 },
    ],
    { duration: Math.min(900, Math.max(420, distance * 2.2)), easing: 'cubic-bezier(.45,.05,.25,1)' },
  );
  const done = () => {
    dot.remove();
    state.packets -= 1;
    pulse(to, kind);
  };
  animation.onfinish = done;
  animation.oncancel = done;
}

function animate(e, p) {
  if (document.hidden) return;
  for (const [from, to, kind, small] of flightsFor(e, p)) fly(from, to, kind, small);
}

// ---------------------------------------------------------------------------
// Chart: webhook responses per second (small multiples, one row per outcome)
// ---------------------------------------------------------------------------

function chartData() {
  const now = Math.floor(Date.now() / 1000);
  const seconds = Array.from({ length: CHART_WINDOW }, (_, i) => now - CHART_WINDOW + 1 + i);
  const count = (map) => {
    if (!map) return 0;
    if (state.scope === 'all') return Object.values(map).reduce((a, b) => a + b, 0);
    return map[state.scope] ?? 0;
  };
  const rows = CHART_SERIES.map((series) => seconds.map((second) => count(state.chart.get(second)?.[series.key])));
  return { seconds, rows };
}

function barPath(x, base, width, height) {
  const r = Math.min(4, width / 2, height);
  const top = base - height;
  return `M${x},${base}V${top + r}Q${x},${top} ${x + r},${top}H${x + width - r}Q${x + width},${top} ${x + width},${top + r}V${base}Z`;
}

function chartIcon(series, cx, cy) {
  const group = svgEl('g', { 'aria-hidden': 'true' });
  group.append(svgEl('circle', { cx, cy, r: 7, class: `icon-${series.tone}` }));
  const paths = {
    check: `M${cx - 3},${cy + 0.2}l2,2.2l4,-4.6`,
    pause: `M${cx - 1.8},${cy - 3}v6M${cx + 1.8},${cy - 3}v6`,
    cross: `M${cx - 2.6},${cy - 2.6}l5.2,5.2M${cx + 2.6},${cy - 2.6}l-5.2,5.2`,
  };
  group.append(svgEl('path', { d: paths[series.glyph], class: `icon-glyph${series.tone === 'warning' ? ' dark' : ''}` }));
  return group;
}

let chartGeometry = null;

function renderChart() {
  const scopeName = state.scope === 'all' ? 'all clients' : clientById(state.scope)?.name ?? state.scope;
  const { seconds, rows } = chartData();
  const max = Math.max(1, ...rows.flat());
  $('#chart-sub').textContent = `Last 60 s · ${scopeName} · tallest bar = ${max}/s`;

  if (state.chartAsTable) {
    renderChartTable(seconds, rows);
    return;
  }

  const svg = $('#chart');
  const width = Math.max(300, Math.floor(svg.parentElement.clientWidth));
  const narrow = width < 560;
  const labelW = narrow ? 118 : 172;
  const totalW = 44;
  const rowH = 36;
  const gap = 14;
  const plotX = labelW;
  const plotW = width - labelW - totalW;
  const rowsH = CHART_SERIES.length * (rowH + gap) - gap;
  const height = rowsH + 24;
  const slot = plotW / CHART_WINDOW;
  const barW = Math.max(1, Math.min(24, slot - 2));
  chartGeometry = { plotX, plotW, slot, rowsH, seconds, rows };

  svg.setAttribute('viewBox', `0 0 ${width} ${height}`);
  svg.setAttribute('height', height);
  const nodes = [];

  if (state.hoverIndex !== null) {
    nodes.push(svgEl('rect', { x: plotX + state.hoverIndex * slot, y: 0, width: slot, height: rowsH, class: 'hover-band' }));
  }

  CHART_SERIES.forEach((series, r) => {
    const top = r * (rowH + gap);
    const base = top + rowH;
    const mid = top + rowH / 2;
    nodes.push(chartIcon(series, 8, mid));
    nodes.push(svgEl('text', { x: 22, y: narrow ? mid + 4 : mid - 2 }, series.label));
    if (!narrow) nodes.push(svgEl('text', { x: 22, y: mid + 13, class: 'row-sub' }, series.sub));
    nodes.push(svgEl('line', { x1: plotX, x2: plotX + plotW, y1: base + 0.5, y2: base + 0.5, class: 'baseline' }));
    rows[r].forEach((value, i) => {
      if (!value) return;
      const barHeight = Math.max(2, (value / max) * (rowH - 4));
      const x = plotX + i * slot + (slot - barW) / 2;
      nodes.push(svgEl('path', { d: barPath(x, base, barW, barHeight), class: `bar-${series.tone}` }));
    });
    const total = rows[r].reduce((a, b) => a + b, 0);
    nodes.push(svgEl('text', { x: width - 2, y: mid + 4, 'text-anchor': 'end', class: 'row-total' }, String(total)));
  });

  for (const ago of [60, 45, 30, 15, 0]) {
    const x = plotX + ((CHART_WINDOW - ago) / CHART_WINDOW) * plotW;
    const anchorPos = ago === 60 ? 'start' : ago === 0 ? 'end' : 'middle';
    nodes.push(svgEl('text', { x, y: rowsH + 18, 'text-anchor': anchorPos, class: 'axis-label' }, ago === 0 ? 'now' : `${ago} s ago`));
  }

  svg.replaceChildren(...nodes);
  svg.setAttribute('aria-label', `Webhook responses per second, last 60 seconds: ${CHART_SERIES.map((s, r) => `${rows[r].reduce((a, b) => a + b, 0)} ${s.label.toLowerCase()}`).join(', ')}`);
  renderTooltip();
}

function renderTooltip() {
  const tip = $('#chart-tooltip');
  if (state.hoverIndex === null || !chartGeometry) {
    tip.hidden = true;
    return;
  }
  const { plotX, slot, seconds, rows } = chartGeometry;
  const i = state.hoverIndex;
  tip.replaceChildren(
    el('div', { class: 'tt-time' }, clock(seconds[i] * 1000, false)),
    ...CHART_SERIES.map((series, r) => el('div', { class: 'tt-row', dataset: { tone: series.tone } }, el('i'), el('strong', {}, rows[r][i]), el('span', {}, series.label.toLowerCase()))),
  );
  tip.hidden = false;
  const wrap = $('#chart-wrap').clientWidth;
  const x = plotX + i * slot + slot / 2;
  let left = x + 12;
  if (left + tip.offsetWidth > wrap) left = x - tip.offsetWidth - 12;
  tip.style.left = `${Math.max(0, left)}px`;
  tip.style.top = '4px';
}

function renderChartTable(seconds, rows) {
  const active = seconds
    .map((second, i) => ({ second, values: rows.map((row) => row[i]) }))
    .filter((row) => row.values.some(Boolean))
    .reverse();
  const table = el(
    'table',
    {},
    el('thead', {}, el('tr', {}, el('th', {}, 'Second'), ...CHART_SERIES.map((s) => el('th', { class: 'num' }, `${s.label} (${s.sub})`)))),
    el(
      'tbody',
      {},
      active.length
        ? active.map((row) => el('tr', {}, el('td', { class: 'mono' }, clock(row.second * 1000, false)), ...row.values.map((v) => el('td', { class: 'num' }, v))))
        : [el('tr', { class: 'empty-row' }, el('td', { colspan: 4 }, 'No webhook responses in the last 60 seconds.'))],
    ),
  );
  $('#chart-table').replaceChildren(table);
}

function setupChartInteraction() {
  const svg = $('#chart');
  const indexAt = (clientX) => {
    if (!chartGeometry) return null;
    const box = svg.getBoundingClientRect();
    const scale = box.width / Number(svg.viewBox.baseVal.width || box.width);
    const x = (clientX - box.left) / scale - chartGeometry.plotX;
    if (x < 0 || x > chartGeometry.plotW) return null;
    return Math.min(CHART_WINDOW - 1, Math.floor(x / chartGeometry.slot));
  };
  svg.addEventListener('pointermove', (event) => {
    const index = indexAt(event.clientX);
    if (index !== state.hoverIndex) {
      state.hoverIndex = index;
      renderChart();
    }
  });
  svg.addEventListener('pointerleave', () => {
    state.hoverIndex = null;
    renderChart();
  });
  svg.addEventListener('focus', () => {
    state.hoverIndex = CHART_WINDOW - 1;
    renderChart();
  });
  svg.addEventListener('blur', () => {
    state.hoverIndex = null;
    renderChart();
  });
  svg.addEventListener('keydown', (event) => {
    if (state.hoverIndex === null) return;
    if (event.key === 'ArrowLeft') state.hoverIndex = Math.max(0, state.hoverIndex - 1);
    else if (event.key === 'ArrowRight') state.hoverIndex = Math.min(CHART_WINDOW - 1, state.hoverIndex + 1);
    else if (event.key === 'Escape') state.hoverIndex = null;
    else return;
    event.preventDefault();
    renderChart();
  });
  $('#chart-view').addEventListener('click', (event) => {
    state.chartAsTable = !state.chartAsTable;
    event.currentTarget.setAttribute('aria-pressed', String(state.chartAsTable));
    event.currentTarget.textContent = state.chartAsTable ? 'Show as chart' : 'Show as table';
    $('#chart-wrap').hidden = state.chartAsTable;
    $('#chart-table').hidden = !state.chartAsTable;
    renderChart();
  });
  let observedWidth = 0;
  new ResizeObserver(([entry]) => {
    const width = Math.floor(entry.contentRect.width);
    if (width === observedWidth) return; // height changes come from renderChart itself
    observedWidth = width;
    renderChart();
  }).observe($('#chart-wrap'));
}

// ---------------------------------------------------------------------------
// Live connection
// ---------------------------------------------------------------------------

function setConnection(stateName, label) {
  $('#conn').dataset.state = stateName;
  $('#conn-label').textContent = label;
}

function resetLocalState() {
  state.events = [];
  state.chart.clear();
  state.holds.clear();
  state.transient.clear();
  $('#log').replaceChildren();
  $('#log-count').textContent = '';
}

function connect() {
  const source = new EventSource('/api/stream');
  source.addEventListener('open', () => setConnection('live', 'Live'));
  source.addEventListener('error', () => setConnection('down', 'Reconnecting…'));
  source.addEventListener('hello', (message) => {
    const { events, snapshot } = JSON.parse(message.data);
    resetLocalState();
    for (const event of events) ingest(event, { live: false });
    if (snapshot) applySnapshot(snapshot);
    renderLog();
    renderChart();
  });
  source.addEventListener('snapshot', (message) => applySnapshot(JSON.parse(message.data)));
  source.addEventListener('sim', (message) => ingest(JSON.parse(message.data), { live: true }));
  source.addEventListener('reset', () => {
    resetLocalState();
    renderChart();
  });
}

// ---------------------------------------------------------------------------
// Start-up
// ---------------------------------------------------------------------------

async function init() {
  applyTheme(storage.get('pns.theme'));
  $('#theme-toggle').addEventListener('click', () => {
    const next = effectiveTheme() === 'dark' ? 'light' : 'dark';
    storage.set('pns.theme', next);
    applyTheme(next);
    renderChart();
  });

  state.meta = await api('/api/meta');
  state.selected = state.meta.scenarios.some((s) => s.id === storage.get('pns.scenario')) ? storage.get('pns.scenario') : state.meta.scenarios[0].id;
  $('#topic-name').textContent = state.meta.topic;
  $('#group-name').textContent = state.meta.consumerGroup;

  buildScenarioList();
  renderParams();
  buildClientControls();
  buildSettingsForm();
  buildLanes();
  buildScope();
  setupChartInteraction();
  renderNarration(true);

  $('#scenario-form').addEventListener('submit', async (event) => {
    event.preventDefault();
    const scenario = currentScenario();
    try {
      await api(`/api/scenarios/${scenario.id}`, { method: 'POST', body: state.params[scenario.id] });
    } catch (err) {
      toast(err.message, 'critical');
    }
  });
  $('#cancel').addEventListener('click', () => api('/api/scenario/cancel', { method: 'POST' }).catch((err) => toast(err.message, 'critical')));
  $('#reset').addEventListener('click', async () => {
    try {
      await api('/api/reset', { method: 'POST' });
      toast('Simulation reset: all clients UP and healthy');
    } catch (err) {
      toast(err.message, 'critical');
    }
  });

  connect();

  setInterval(() => {
    if (!state.snapshot) return;
    for (const p of state.lanes.keys()) renderWorker(p);
    updateElapsed();
  }, 150);
  setInterval(() => {
    const cutoff = Math.floor(Date.now() / 1000) - CHART_WINDOW * 2;
    for (const second of state.chart.keys()) if (second < cutoff) state.chart.delete(second);
    renderChart();
  }, 1000);
}

init().catch((err) => {
  setConnection('down', 'Failed to load');
  toast(`Could not start the UI: ${err.message}`, 'critical');
});
