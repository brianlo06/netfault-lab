/* NetFault Lab — replay player.
 *
 * Loads a captured run (the proxy's real JSON Lines event log plus a time
 * series of its metrics documents) and folds both into keyframes. Rendering at
 * any position is then a binary search, so scrubbing is exact and cannot drift
 * out of step with playback.
 *
 * Nothing here invents data: every queue depth, counter and log line comes from
 * the capture. Values between two samples are held, never interpolated.
 */

'use strict';

const el = (id) => document.getElementById(id);

const ui = {
  tabs: el('scenarioTabs'),
  summary: el('scenarioSummary'),
  commands: el('commands'),
  c2uFill: el('c2uFill'), u2cFill: el('u2cFill'),
  c2uBytes: el('c2uBytes'), u2cBytes: el('u2cBytes'),
  c2uPause: el('c2uPause'), u2cPause: el('u2cPause'),
  c2uLow: el('c2uLow'), c2uHigh: el('c2uHigh'),
  u2cLow: el('u2cLow'), u2cHigh: el('u2cHigh'),
  playBtn: el('playBtn'), scrub: el('scrub'),
  clockNow: el('clockNow'), clockEnd: el('clockEnd'),
  speed: el('speed'), readouts: el('readouts'),
  log: el('log'), showArtifacts: el('showArtifacts'),
};

const PLAY_ICON = '<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><path d="M4 2.5v11l9-5.5z" fill="currentColor"/></svg>';
const PAUSE_ICON = '<svg viewBox="0 0 16 16" width="16" height="16" aria-hidden="true"><rect x="4" y="2.5" width="3" height="11" fill="currentColor"/><rect x="9" y="2.5" width="3" height="11" fill="currentColor"/></svg>';

const state = {
  scenarios: [],
  cache: new Map(),
  run: null,       // { keyframes, events, duration, config, doc }
  playhead: 0,
  playing: false,
  speed: 1,
  rafId: null,
  lastTs: 0,
  renderedLogCount: -1,
};

/* ---------- helpers ---------- */

function formatBytes(n) {
  if (n === null || n === undefined) return '—';
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(n < 10240 ? 1 : 0)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
}

function formatSeconds(ms) {
  return (ms / 1000).toFixed(3);
}

/** Parse the proxy's `key=value,key=value` detail string. */
function parseDetail(detail) {
  const out = {};
  if (!detail) return out;
  for (const part of String(detail).split(',')) {
    const eq = part.indexOf('=');
    if (eq === -1) { out.reason = part; continue; }
    const key = part.slice(0, eq);
    const value = part.slice(eq + 1);
    const num = Number(value);
    out[key] = value !== '' && Number.isFinite(num) ? num : value;
  }
  return out;
}

function eventKind(name) {
  if (name === 'read_paused') return 'pause';
  if (name === 'read_resumed') return 'resume';
  if (name === 'fault_injected') return 'fault';
  if (name === 'connection_closed' || name === 'half_close_forwarded' || name === 'peer_half_closed') return 'close';
  if (name === 'connection_accepted' || name === 'upstream_connected' || name === 'proxy_started') return 'open';
  return 'other';
}

/* ---------- timeline construction ---------- */

/**
 * Fold events and metrics samples into keyframes ordered by time.
 * Each keyframe carries the complete visible state at that instant, so
 * rendering never depends on replay order.
 */
function buildRun(doc) {
  const events = doc.events || [];
  const samples = doc.metrics_samples || [];
  if (!events.length) return null;

  const origin = events[0].timestamp_ms;
  const marks = [];

  events.forEach((event, index) => {
    marks.push({ t: event.timestamp_ms - origin, order: index, type: 'event', event, index });
  });
  samples.forEach((sample, index) => {
    marks.push({ t: sample.timestamp_ms - origin, order: 1e6 + index, type: 'sample', sample });
  });
  marks.sort((a, b) => (a.t - b.t) || (a.order - b.order));

  // The workload connection is the one that actually moved bytes; captures are
  // taken without a port probe, so in practice there is exactly one.
  let workloadId = null;
  for (const event of events) {
    if (event.event === 'connection_closed' && event.connection_id) { workloadId = event.connection_id; break; }
  }
  if (workloadId === null) {
    for (const event of events) { if (event.connection_id) { workloadId = event.connection_id; break; } }
  }

  const current = {
    t: 0,
    connState: 'waiting',
    c2uQueue: 0, u2cQueue: 0,
    c2uPaused: false, u2cPaused: false,
    clientRead: 0, clientWritten: 0, upstreamRead: 0, upstreamWritten: 0,
    pauseCount: 0, delayedSegments: 0, delayBudgetUs: 0,
    eagain: 0, partialWrites: 0,
    eventCount: 0,
    finalState: null,
  };
  const keyframes = [];

  for (const mark of marks) {
    current.t = mark.t;

    if (mark.type === 'event') {
      const { event } = mark;
      current.eventCount = mark.index + 1;
      const detail = parseDetail(event.detail);
      const mine = !event.connection_id || event.connection_id === workloadId;

      if (mine && event.state && event.connection_id) current.connState = event.state;

      if (mine && event.event === 'read_paused') {
        if (detail.direction === 'client_to_upstream') { current.c2uPaused = true; current.c2uQueue = detail.queue_bytes ?? current.c2uQueue; }
        else { current.u2cPaused = true; current.u2cQueue = detail.queue_bytes ?? current.u2cQueue; }
        current.pauseCount += 1;
      } else if (mine && event.event === 'read_resumed') {
        if (detail.direction === 'client_to_upstream') { current.c2uPaused = false; current.c2uQueue = detail.queue_bytes ?? current.c2uQueue; }
        else { current.u2cPaused = false; current.u2cQueue = detail.queue_bytes ?? current.u2cQueue; }
      } else if (mine && event.event === 'connection_closed') {
        current.finalState = event.state;
        current.c2uQueue = 0; current.u2cQueue = 0;
        current.c2uPaused = false; current.u2cPaused = false;
        if (typeof detail.client_read === 'number') {
          current.clientRead = detail.client_read;
          current.clientWritten = detail.client_written;
          current.upstreamRead = detail.upstream_read;
          current.upstreamWritten = detail.upstream_written;
          current.eagain = detail.eagain_events ?? current.eagain;
          current.partialWrites = detail.partial_writes ?? current.partialWrites;
          current.delayedSegments = (detail.c2u_delayed_segments ?? 0) + (detail.u2c_delayed_segments ?? 0);
          current.delayBudgetUs = (detail.c2u_delay_budget_us ?? 0) + (detail.u2c_delay_budget_us ?? 0);
        }
      }
    } else {
      const connection = (mark.sample.connections || []).find((c) => c.id === workloadId);
      if (connection) {
        current.connState = connection.state;
        current.c2uQueue = connection.c2u_queue_bytes;
        current.u2cQueue = connection.u2c_queue_bytes;
        current.clientRead = connection.client_bytes_read;
        current.clientWritten = connection.client_bytes_written;
        current.upstreamRead = connection.upstream_bytes_read;
        current.upstreamWritten = connection.upstream_bytes_written;
        current.eagain = connection.eagain_events;
        current.partialWrites = connection.partial_writes;
        current.delayedSegments = connection.c2u_delayed_segments + connection.u2c_delayed_segments;
        current.delayBudgetUs = connection.c2u_delay_budget_us + connection.u2c_delay_budget_us;
      }
    }
    keyframes.push({ ...current });
  }

  const duration = keyframes[keyframes.length - 1].t;
  return {
    doc,
    keyframes,
    events,
    duration: Math.max(duration, 1),
    config: doc.config || { queue_capacity_bytes: 65536, low_water_bytes: 32768, high_water_bytes: 65536 },
  };
}

function frameAt(run, t) {
  const frames = run.keyframes;
  let lo = 0, hi = frames.length - 1, best = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (frames[mid].t <= t) { best = mid; lo = mid + 1; } else { hi = mid - 1; }
  }
  return frames[best];
}

/* ---------- rendering ---------- */

function renderQueue(fillEl, bytesEl, pauseEl, value, capacity, paused) {
  const pct = capacity > 0 ? Math.min(100, (value / capacity) * 100) : 0;
  fillEl.style.width = `${pct}%`;
  fillEl.classList.toggle('paused', paused);
  bytesEl.textContent = formatBytes(value);
  pauseEl.hidden = !paused;
}

function renderWatermarks(config) {
  const capacity = config.queue_capacity_bytes || 1;
  const lowPct = Math.min(100, (config.low_water_bytes / capacity) * 100);
  const highPct = Math.min(100, (config.high_water_bytes / capacity) * 100);
  for (const [low, high] of [[ui.c2uLow, ui.c2uHigh], [ui.u2cLow, ui.u2cHigh]]) {
    low.style.left = `${lowPct}%`;
    high.style.left = `${highPct}%`;
    // A high-water mark at capacity would sit under the border; nudge it in.
    high.style.transform = highPct >= 99.5 ? 'translateX(-2px)' : 'none';
  }
}

function renderReadouts(frame, run) {
  const stateLabel = frame.finalState || frame.connState;
  const rows = [
    ['connection', stateLabel.replace(/_/g, ' '), `state-${stateLabel}`],
    ['client → proxy', formatBytes(frame.clientRead), ''],
    ['proxy → upstream', formatBytes(frame.upstreamWritten), ''],
    ['upstream → proxy', formatBytes(frame.upstreamRead), ''],
    ['proxy → client', formatBytes(frame.clientWritten), ''],
    ['read pauses', String(frame.pauseCount), ''],
  ];
  if (frame.delayedSegments > 0) {
    rows.push(['delayed chunks', String(frame.delayedSegments), '']);
    rows.push(['injected delay', `${(frame.delayBudgetUs / 1000).toFixed(0)} ms`, '']);
  }
  ui.readouts.innerHTML = rows.map(([label, value, cls]) =>
    `<dl class="readout"><dt>${label}</dt><dd class="${cls}">${value}</dd></dl>`
  ).join('');
}

function renderLog(run, frame) {
  const showArtifacts = ui.showArtifacts.checked;
  const upTo = frame.eventCount;
  if (state.renderedLogCount === upTo && state.renderedArtifacts === showArtifacts) return;
  state.renderedLogCount = upTo;
  state.renderedArtifacts = showArtifacts;

  const origin = run.events[0].timestamp_ms;
  const lines = [];
  for (let i = 0; i < upTo; i++) {
    const event = run.events[i];
    if (event.capture_artifact && !showArtifacts) continue;
    const t = ((event.timestamp_ms - origin) / 1000).toFixed(3);
    const detail = event.detail ? String(event.detail) : '';
    lines.push(
      `<div class="log-line${event.capture_artifact ? ' is-artifact' : ''}" data-kind="${eventKind(event.event)}">` +
      `<span class="t">${t}</span><span class="e">${event.event}</span><span class="d">${detail}</span></div>`
    );
  }
  ui.log.innerHTML = lines.join('');
  ui.log.scrollTop = ui.log.scrollHeight;
}

function render() {
  const run = state.run;
  if (!run) return;
  const frame = frameAt(run, state.playhead);
  const capacity = run.config.queue_capacity_bytes;

  renderQueue(ui.c2uFill, ui.c2uBytes, ui.c2uPause, frame.c2uQueue, capacity, frame.c2uPaused);
  renderQueue(ui.u2cFill, ui.u2cBytes, ui.u2cPause, frame.u2cQueue, capacity, frame.u2cPaused);
  renderReadouts(frame, run);
  renderLog(run, frame);

  ui.clockNow.textContent = formatSeconds(state.playhead);
  ui.scrub.value = String(Math.round((state.playhead / run.duration) * 1000));
}

/* ---------- playback ---------- */

function setPlaying(on) {
  state.playing = on;
  ui.playBtn.innerHTML = on ? PAUSE_ICON : PLAY_ICON;
  ui.playBtn.setAttribute('aria-label', on ? 'Pause' : 'Play');
  if (on) {
    state.lastTs = performance.now();
    state.rafId = requestAnimationFrame(step);
  } else if (state.rafId) {
    cancelAnimationFrame(state.rafId);
    state.rafId = null;
  }
}

function step(ts) {
  if (!state.playing || !state.run) return;
  const delta = (ts - state.lastTs) * state.speed;
  state.lastTs = ts;
  state.playhead += delta;
  if (state.playhead >= state.run.duration) {
    state.playhead = state.run.duration;
    render();
    setPlaying(false);
    return;
  }
  render();
  state.rafId = requestAnimationFrame(step);
}

function seek(ms, { pause = true } = {}) {
  if (pause && state.playing) setPlaying(false);
  state.playhead = Math.max(0, Math.min(ms, state.run ? state.run.duration : 0));
  state.renderedLogCount = -1; // scrubbing backwards must rebuild the log
  render();
}

/* ---------- scenario loading ---------- */

function renderCommands(doc) {
  const rows = [
    ['server', doc.command.server],
    ['proxy', doc.command.proxy],
    ['client', doc.command.client],
  ];
  ui.commands.innerHTML = rows
    .map(([name, cmd]) => `<div class="cmd"><b>$</b> ${cmd.replace(/</g, '&lt;')}</div>`)
    .join('');
}

async function selectScenario(key) {
  for (const button of ui.tabs.querySelectorAll('button')) {
    button.setAttribute('aria-selected', String(button.dataset.key === key));
  }
  const meta = state.scenarios.find((s) => s.key === key);
  ui.summary.textContent = meta ? meta.summary : '';

  let doc = state.cache.get(key);
  if (!doc) {
    ui.commands.innerHTML = '<div class="cmd">loading capture…</div>';
    try {
      const response = await fetch(`data/${key}.json`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      doc = await response.json();
    } catch (error) {
      ui.commands.innerHTML = `<div class="cmd">could not load this capture (${error.message})</div>`;
      return;
    }
    state.cache.set(key, doc);
  }

  setPlaying(false);
  state.run = buildRun(doc);
  if (!state.run) return;
  state.playhead = 0;
  state.renderedLogCount = -1;

  renderCommands(doc);
  renderWatermarks(state.run.config);
  ui.clockEnd.textContent = formatSeconds(state.run.duration);
  render();
  setPlaying(true);
}

async function init() {
  ui.playBtn.innerHTML = PLAY_ICON;

  let index;
  try {
    const response = await fetch('data/index.json');
    index = await response.json();
  } catch (error) {
    ui.summary.textContent = 'Could not load the captured runs.';
    return;
  }
  state.scenarios = index.scenarios || [];

  ui.tabs.innerHTML = state.scenarios.map((s, i) =>
    `<button role="tab" data-key="${s.key}" aria-selected="${i === 0}">${s.title}</button>`
  ).join('');
  ui.tabs.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-key]');
    if (button) selectScenario(button.dataset.key);
  });

  ui.playBtn.addEventListener('click', () => {
    if (!state.run) return;
    if (!state.playing && state.playhead >= state.run.duration) seek(0, { pause: false });
    setPlaying(!state.playing);
  });

  ui.scrub.addEventListener('input', () => {
    if (!state.run) return;
    seek((Number(ui.scrub.value) / 1000) * state.run.duration);
  });

  ui.speed.addEventListener('change', () => { state.speed = Number(ui.speed.value); });

  ui.showArtifacts.addEventListener('change', () => {
    state.renderedLogCount = -1;
    render();
  });

  if (state.scenarios.length) await selectScenario(state.scenarios[0].key);
}

init();
