'use strict';
// webterm — VS Code-style web terminal: real PTYs decoupled from browser
// connections. Sessions survive refresh; recent output is replayed on attach.
// Protocol: text frames = JSON control, binary frames = terminal output.
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const pty = require('node-pty');

const PORT = process.env.PORT || 7682;
const SHELL_CMD = process.env.SHELL_CMD || '/bin/bash -l';
const REPLAY_LIMIT = 1024 * 1024; // keep last 1 MiB of output per session

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript',
  '.css': 'text/css',
};
const PUBLIC = path.join(__dirname, 'public');
const STATIC = {};
for (const file of fs.readdirSync(PUBLIC)) {
  STATIC['/' + file] = fs.readFileSync(path.join(PUBLIC, file));
}

// session = { id, proc, chunks: [Buffer], size, clients: Set<ws> }
const sessions = new Map();

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
  const session = { id: crypto.randomBytes(8).toString('hex'), proc, chunks: [], size: 0, clients: new Set() };
  proc.onData((data) => {
    appendOutput(session, data);
    for (const ws of session.clients) {
      if (!ws.paused) safeSend(ws, Buffer.from(data));
    }
  });
  proc.onExit(({ exitCode, signal }) => {
    console.log(`[webterm] session ${session.id} exited code=${exitCode} signal=${signal}`);
    for (const ws of session.clients) safeSend(ws, JSON.stringify({ type: "exit", code: exitCode, signal }));
    sessions.delete(session.id);
  });
  console.log(`[webterm] session ${session.id} spawned: ${SHELL_CMD}`);
  return session;
}

function attach(ws, session, cols, rows) {
  ws.session = session;
  session.clients.add(ws);
  session.proc.resize(cols, rows);
  safeSend(ws, JSON.stringify({ type: "init", id: session.id, replay: session.chunks.length > 0 }));
  if (session.chunks.length > 0) safeSend(ws, Buffer.concat(session.chunks));
  console.log(`[webterm] session ${session.id} attached (clients=${session.clients.size})`);
}

const server = http.createServer((req, res) => {
  const file = STATIC[req.url] ? req.url : '/index.html';
  res.writeHead(200, { 'Content-Type': MIME[path.extname(file)] || 'text/html' });
  res.end(STATIC[file]);
});

const wss = new WebSocketServer({ server, path: '/ws' });

function safeSend(ws, data) {
  if (ws.readyState === ws.OPEN) ws.send(data);
}

wss.on('connection', (ws) => {
  ws.paused = false;
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
        case 'pause': ws.paused = true; break;
        case 'resume': ws.paused = false; break;
        case 'close': ws.session.proc.kill(); break;
      }
    }
  });
  ws.on('close', () => {
    if (ws.session) {
      ws.session.clients.delete(ws);
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
