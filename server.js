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

const PORT = process.env.PORT || 7682;
const SHELL_CMD = process.env.SHELL_CMD || '/bin/bash -l';
const TITLE = process.env.TITLE || 'webterm';
const REPLAY_LIMIT = 1024 * 1024; // keep last 1 MiB of output per session
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

// session = { id, proc, chunks: [Buffer], size, clients: Set<ws>, flowPaused, flowCheckScheduled }
const sessions = new Map();

function scheduleFlowCheck(session) {
  if (session.flowCheckScheduled) return;
  session.flowCheckScheduled = true;
  setTimeout(() => {
    session.flowCheckScheduled = false;
    recomputeFlow(session);
  }, 300);
}

// bytes handed to ws.send (bufferedAmount) + still queued, per client
function outstanding(ws) {
  return ws.qBytes + ws.bufferedAmount;
}

function recomputeFlow(session) {
  if (!sessions.has(session.id)) return; // session exited
  const anyOver = [...session.clients].some((c) => outstanding(c) > Q_HIGH);
  const allLow = [...session.clients].every((c) => outstanding(c) <= Q_LOW);
  if (anyOver && !session.flowPaused) {
    session.flowPaused = true;
    session.proc.pause();
  } else if (allLow && session.flowPaused) {
    session.flowPaused = false;
    session.proc.resume();
  }
  if (session.flowPaused) {
    // paused pty emits no onData, so keep rechecking until the client drains
    scheduleFlowCheck(session);
  }
}

function pump(ws) {
  if (ws.pumping || !ws.session) return;
  ws.pumping = true;
  while (ws.q.length && ws.bufferedAmount < Q_HIGH && ws.readyState === ws.OPEN) {
    const item = ws.q.shift();
    ws.qBytes -= item.bytes;
    ws.send(item.data, () => {
      // chunk flushed to the socket — keep draining the queue, then re-evaluate
      if (!ws.session) return;
      pump(ws);
      recomputeFlow(ws.session);
    });
  }
  ws.pumping = false;
  recomputeFlow(ws.session);
}

// one ordered frame queue per client: text (control) and binary (output)
// frames keep their order, so 'live'/'exit' can never overtake pending output
function enqueue(ws, data) {
  const bytes = typeof data === 'string' ? Buffer.byteLength(data) : data.length;
  ws.q.push({ data, bytes });
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
    flowCheckScheduled: false,
  };
  proc.onData((data) => {
    appendOutput(session, data);
    const buf = Buffer.from(data);
    for (const ws of session.clients) enqueue(ws, buf);
    recomputeFlow(session);
  });
  proc.onExit(({ exitCode, signal }) => {
    console.log(`[webterm] session ${session.id} exited code=${exitCode} signal=${signal}`);
    for (const ws of session.clients) {
      enqueue(ws, JSON.stringify({ type: "exit", code: exitCode, signal }));
    }
    sessions.delete(session.id);
  });
  console.log(`[webterm] session ${session.id} spawned: ${SHELL_CMD}`);
  return session;
}

function attach(ws, session, cols, rows) {
  ws.session = session;
  session.clients.add(ws);
  session.proc.resize(cols, rows);
  const hasReplay = session.chunks.length > 0;
  enqueue(ws, JSON.stringify({ type: "init", id: session.id, replay: hasReplay }));
  if (hasReplay) {
    enqueue(ws, JSON.stringify({ type: "replay" }));
    enqueue(ws, Buffer.concat(session.chunks));
    enqueue(ws, JSON.stringify({ type: "live" }));
  }
  console.log(`[webterm] session ${session.id} attached (clients=${session.clients.size})`);
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
      attach(ws, session, msg.cols, msg.rows);
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
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

server.listen(PORT, () => console.log(`[webterm] listening on :${PORT}`));
