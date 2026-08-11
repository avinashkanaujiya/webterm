'use strict';
// webterm — VS Code-style web terminal: real PTYs decoupled from browser
// connections. Sessions survive refresh; recent output is replayed on attach.
// Protocol: text frames = JSON control, binary frames = terminal output.
//
// Flow control (mirrors VS Code's ptyHost): output is queued per client with a
// bounded watermark; when any client falls behind the PTY is paused (the shell
// blocks on the pty buffer — nothing is dropped), and resumed when caught up.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const pty = require('node-pty');
const { Terminal: HeadlessTerminal } = require('@xterm/headless');

const PORT = process.env.PORT || 7682;
const SHELL_CMD = process.env.SHELL_CMD || '/bin/bash -l';
const TITLE = process.env.TITLE || 'webterm';
const REPLAY_LIMIT = 1024 * 1024; // keep last 1 MiB of output per session
const REPLAY_FRAME = 256 * 1024; // split replay into <= 256 KiB ws frames
const Q_HIGH = 1024 * 1024; // per-client outstanding bytes: pause the pty above this
const Q_LOW = 256 * 1024; // per-client outstanding bytes: resume when all below this

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.css': 'text/css',
  '.svg': 'image/svg+xml',
};
const PUBLIC = path.join(__dirname, 'public');
const STATIC = {};
for (const file of fs.readdirSync(PUBLIC)) {
  let content = fs.readFileSync(path.join(PUBLIC, file));
  if (file === 'index.html') content = content.toString().replaceAll('__TITLE__', TITLE);
  STATIC['/' + file] = content;
}

// session = { id, proc, chunks: [Buffer], size, clients: Set<ws>, flowPaused, pausedSince, flowTimer }
const sessions = new Map();

const FLOW_TICK_MS = 500;
const FLOW_FORCE_RESUME_MS = 10000; // pty paused this long while drained -> retry resume

// bytes handed to ws.send (bufferedAmount) + still queued (qBytes) + backed
// up in the socket's own write queue (writableLength) + live frames held
// during attach (heldBytes) — without the socket queue the metric stays ~0
// under congestion and flow control never engages
function outstanding(ws) {
  return ws.qBytes + (ws.heldBytes || 0) + ws.bufferedAmount + (ws._socket ? ws._socket.writableLength : 0);
}

// pure flow decision over client-like objects ({ qBytes, bufferedAmount,
// _socket: { writableLength }, replayPending }) — exported for unit tests.
// replay bytes still in flight are catch-up, not live backlog: subtract them
// so the watermark bounds each client's LIVE output (bounded-queue contract),
// while a slow replay drain alone never freezes the pty
function flowDecision(clients) {
  const live = (c) => outstanding(c) - (c.replayPending || 0);
  const anyOver = clients.some((c) => live(c) > Q_HIGH);
  const allLow = clients.every((c) => live(c) <= Q_LOW);
  return { anyOver, allLow };
}

function recomputeFlow(session) {
  if (!sessions.has(session.id)) return; // session exited
  const { anyOver, allLow } = flowDecision([...session.clients]);
  const now = Date.now();
  if (anyOver && !session.flowPaused) {
    session.flowPaused = true;
    session.pausedSince = now;
    session.proc.pause();
    console.log(`[flow] ${session.id.slice(0, 6)} paused`);
  } else if (allLow && session.flowPaused) {
    session.flowPaused = false;
    session.proc.resume();
    console.log(`[flow] ${session.id.slice(0, 6)} resumed after ${now - session.pausedSince}ms`);
  } else if (session.flowPaused && allLow && now - session.pausedSince > FLOW_FORCE_RESUME_MS) {
    // normal resume path should have fired; retry in case node-pty's read
    // stream didn't restart — self-heal instead of leaving the pty frozen
    session.flowPaused = false;
    session.proc.resume();
    console.log(`[flow] ${session.id.slice(0, 6)} FORCED resume after ${now - session.pausedSince}ms`);
  } else if (session.flowPaused && now - session.pausedSince > FLOW_FORCE_RESUME_MS * 3) {
    console.log(`[flow] ${session.id.slice(0, 6)} still paused ${now - session.pausedSince}ms outstanding=[${[...session.clients].map((c) => outstanding(c)).join(',')}]`);
  }
}

// deterministic per-session tick: pump queues + re-evaluate flow. Runs on a
// timer so pause/resume decisions never depend on ws callbacks (whose loss
// previously left the pty paused forever after heavy agent output)
function flowTick(session) {
  for (const ws of session.clients) pump(ws);
  recomputeFlow(session);
}

function pump(ws) {
  if (ws.pumping || !ws.session) return;
  ws.pumping = true;
  // urgent frames (the attach/replay burst) bypass the watermark — they are
  // bounded at REPLAY_LIMIT total, but a queued replay >= Q_HIGH could never
  // pass the outstanding() < Q_HIGH gate and would wedge the queue forever
  while (ws.q.length && ws.readyState === ws.OPEN && (ws.q[0].urgent || outstanding(ws) < Q_HIGH)) {
    const item = ws.q.shift();
    ws.qBytes -= item.bytes;
    ws.send(item.data, () => {
      // replay bytes leave the in-flight count as their frames flush; once
      // the last urgent frame (the live marker) is flushed, replayPending is
      // 0 and the client is accounted normally again
      if (item.urgent && ws.replayPending) ws.replayPending -= item.bytes;
      if (ws.session) pump(ws); // best-effort immediate drain; flowTick is authoritative
    });
  }
  ws.pumping = false;
}

// one ordered frame queue per client: text (control) and binary (output)
// frames keep their order, so 'live'/'exit' can never overtake pending output.
// While holdReplay is set (attach computing its replay split), frames park in
// ws.held instead of the queue and are flushed after the burst — output the
// pty emitted mid-computation must follow the replay, never precede it.
function enqueue(ws, data, urgent) {
  const bytes = typeof data === 'string' ? Buffer.byteLength(data) : data.length;
  if (ws.holdReplay) {
    ws.held.push(data);
    ws.heldBytes += bytes;
    return;
  }
  ws.q.push({ data, bytes, urgent: !!urgent });
  ws.qBytes += bytes;
  pump(ws);
}

function appendOutput(session, data) {
  const buf = Buffer.from(data);
  session.chunks.push(buf);
  session.size += buf.length;
  while (session.size > REPLAY_LIMIT) {
    const head = session.chunks[0];
    const drop = session.size - REPLAY_LIMIT;
    if (session.chunks.length === 1 || head.length > drop) {
      // single oversized chunk (or the head alone covers the overflow): trim it
      session.chunks[0] = head.subarray(Math.min(drop, head.length));
      session.size = REPLAY_LIMIT;
      break;
    }
    session.size -= head.length;
    session.chunks.shift();
  }
}

function spawnSession() {
  const proc = pty.spawn('/bin/bash', ['-lc', SHELL_CMD], {
    name: 'xterm-256color',
    cols: 80,
    rows: 24,
    cwd: '/',
    env: { ...process.env, TERM: 'xterm-256color', COLORTERM: 'truecolor', LANG: 'C.UTF-8', USER: 'root' },
  });
  const session = {
    id: crypto.randomBytes(8).toString('hex'),
    proc,
    chunks: [],
    size: 0,
    clients: new Set(),
    flowPaused: false,
    pausedSince: 0,
    flowTimer: setInterval(() => flowTick(session), FLOW_TICK_MS),
  };
  proc.onData((data) => {
    appendOutput(session, data);
    const buf = Buffer.from(data);
    for (const ws of session.clients) enqueue(ws, buf);
    recomputeFlow(session);
  });
  proc.onExit(({ exitCode, signal }) => {
    console.log(`[webterm] session ${session.id} exited code=${exitCode} signal=${signal}`);
    clearInterval(session.flowTimer);
    for (const ws of session.clients) {
      enqueue(ws, JSON.stringify({ type: 'exit', code: exitCode, signal }));
    }
    sessions.delete(session.id);
  });
  console.log(`[webterm] session ${session.id} spawned: ${SHELL_CMD}`);
  return session;
}

// --- replay preview split --------------------------------------------------
// The ring is a raw byte stream: the current screen is a function of the whole
// stream in order, so it cannot be reordered client-side. To paint the current
// screen FIRST on refresh we find the shortest byte suffix that, replayed into
// a fresh emulator, reproduces the final screen exactly (text + colors +
// cursor + normal/alt buffer). That suffix is self-contained: the client can
// render it immediately (the preview) while the older prefix streams in behind
// it. @xterm/headless is the same emulator core as the client's xterm.js, so
// its parsing semantics match by construction. scrollback: 0 keeps these
// terminals to `rows` lines — only the final screen is compared.
async function writeAll(t, data) {
  return new Promise((resolve) => t.write(data, resolve));
}

function headlessSnapshot(t, cols, rows) {
  const core = t._core;
  const buf = core.buffer;
  const lines = buf.lines;
  const n = lines.length;
  const screen = [];
  const attrs = [];
  for (let i = 0; i < rows; i++) {
    const l = lines.get(n - rows + i);
    if (!l) {
      screen.push('');
      attrs.push('');
      continue;
    }
    screen.push(l.translateToString(true));
    let a = '';
    for (let j = 0; j < cols; j++) a += l.getFg(j) + ',' + l.getBg(j) + ';';
    attrs.push(a);
  }
  return { alt: buf === core.buffers.alt, cx: buf.x, cy: buf.y, screen, attrs };
}

function sameScreen(a, b) {
  if (a.alt !== b.alt || a.cx !== b.cx || a.cy !== b.cy) return false;
  for (let i = 0; i < a.screen.length; i++) {
    if (a.screen[i] !== b.screen[i] || a.attrs[i] !== b.attrs[i]) return false;
  }
  return true;
}

// Earliest byte offset whose suffix reproduces the final screen, or 0 when no
// suffix up to half the ring does (preview the whole ring then — old behavior).
async function findBoundary(total, cols, rows) {
  const opts = { cols, rows, scrollback: 0, allowProposedApi: true };
  const full = new HeadlessTerminal(opts);
  await writeAll(full, total);
  const want = headlessSnapshot(full, cols, rows);
  for (let dist = 512; dist < total.length; dist *= 2) {
    const t = new HeadlessTerminal(opts);
    await writeAll(t, total.subarray(total.length - dist));
    if (sameScreen(headlessSnapshot(t, cols, rows), want)) return total.length - dist;
  }
  return 0;
}

async function attach(ws, session, cols, rows) {
  ws.session = session;
  session.clients.add(ws);
  // park any output the pty emits while the replay split is computed; it is
  // flushed after the burst so the replay never interleaves with live frames
  ws.held = [];
  ws.heldBytes = 0;
  ws.holdReplay = true;
  session.proc.resize(cols, rows);
  const hasReplay = session.chunks.length > 0;
  let total = null;
  let boundary = 0;
  if (hasReplay) {
    total = Buffer.concat(session.chunks); // <= REPLAY_LIMIT
    boundary = await findBoundary(total, cols, rows);
  }
  ws.holdReplay = false;
  const initMsg = JSON.stringify({ type: "init", id: session.id, replay: hasReplay });
  enqueue(ws, initMsg, true);
  if (hasReplay) {
    const replayMsg = JSON.stringify({ type: "replay" });
    const fillMsg = JSON.stringify({ type: "fill" });
    const liveMsg = JSON.stringify({ type: "live" });
    const suffix = total.subarray(boundary); // newest content first (preview)
    // in-flight replay bytes (excluded from the live watermark until flushed)
    ws.replayPending = Buffer.byteLength(initMsg) + Buffer.byteLength(replayMsg)
      + suffix.length + Buffer.byteLength(fillMsg) + total.length + Buffer.byteLength(liveMsg);
    enqueue(ws, replayMsg, true);
    // split into <= REPLAY_FRAME pieces: one giant frame >= Q_HIGH would wedge
    // the pump gate, and small frames let the browser breathe between parses
    for (let off = 0; off < suffix.length; off += REPLAY_FRAME) {
      enqueue(ws, suffix.subarray(off, Math.min(off + REPLAY_FRAME, suffix.length)), true);
    }
    enqueue(ws, fillMsg, true);
    // the full ring in order: the client's hidden terminal rebuilds the exact
    // end state (history + screen), then replaces the preview wholesale
    for (let off = 0; off < total.length; off += REPLAY_FRAME) {
      enqueue(ws, total.subarray(off, Math.min(off + REPLAY_FRAME, total.length)), true);
    }
    enqueue(ws, liveMsg, true); // last burst frame
  }
  // flush what the pty emitted while the split was computed, after the burst
  const held = ws.held;
  ws.held = [];
  ws.heldBytes = 0;
  for (const frame of held) enqueue(ws, frame, false);
  pump(ws);
  console.log(`[webterm] session ${session.id} attached (clients=${session.clients.size} replay=${hasReplay} boundary=${boundary}/${total ? total.length : 0})`);
}

const server = http.createServer((req, res) => {
  const file = STATIC[req.url] ? req.url : '/index.html';
  res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'text/html' });
  res.end(STATIC[file]);
});

// perMessageDeflate off: terminal streams are latency-sensitive and per-frame
// deflate adds CPU + jitter (ttyd sends raw bytes; so do we now)
const wss = new WebSocketServer({ server, path: '/ws', perMessageDeflate: false });

wss.on('connection', (ws) => {
  ws._socket.setNoDelay(true); // no Nagle on terminal traffic
  ws.q = [];
  ws.qBytes = 0;
  ws.replayPending = 0;
  ws.pumping = false;
  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (msg.type === 'hello') {
      const session = (msg.id && sessions.get(msg.id)) || spawnSession();
      if (!sessions.has(session.id)) sessions.set(session.id, session);
      attach(ws, session, msg.cols, msg.rows).catch((err) => {
        console.error(`[webterm] attach failed for ${session.id}:`, err);
        ws.close();
      });
    } else if (ws.session) {
      switch (msg.type) {
        case 'input': ws.session.proc.write(msg.data); break;
        case 'resize': ws.session.proc.resize(msg.cols, msg.rows); break;
        case 'close': ws.session.proc.kill(); break;
      }
    }
  });
  ws.on('close', () => {
    if (ws.session) {
      ws.session.clients.delete(ws);
      ws.q.length = 0;
      ws.qBytes = 0;
      recomputeFlow(ws.session);
      console.log(`[webterm] session ${ws.session.id} detached (clients=${ws.session.clients.size})`);
    }
  });
});

function shutdown() {
  console.log('[webterm] shutting down, killing sessions');
  for (const s of sessions.values()) s.proc.kill();
  process.exit(0);
}

if (require.main === module) {
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
  server.listen(PORT, () => console.log(`[webterm] listening on :${PORT}`));
} else {
  module.exports = { flowDecision, outstanding, Q_HIGH, Q_LOW, REPLAY_LIMIT };
}