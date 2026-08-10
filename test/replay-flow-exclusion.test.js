'use strict';
// Regression: a client that re-attaches and drains the full 1 MiB replay must
// not freeze the PTY for everyone else while it catches up. S is a slow
// consumer (per-frame busy-wait throttle — the skill's documented pattern; no
// ws.pause() wedge), so the replay stays in flight in the server's socket
// buffers. A fast client F attached to the same session must still stream a
// moderate flood end to end.
//
// Guards the replayPending accounting: replay bytes in flight are subtracted
// from the flow watermark (bounded, one-time catch-up), while each client's
// LIVE backlog still counts — a moderate live flood (~750 KB, under the
// 1 MiB watermark) must not trigger a pause. If replay bytes counted toward
// the watermark, S's raw outstanding (replay in flight + flood) would cross
// it and the pty would pause, stalling F mid-flood.
//
// S runs in a CHILD PROCESS: its busy-waits must not block F's event loop,
// or the measurement itself inherits S's throttle.
//
// Run against any running webterm instance:
//   WEBTERM_URL=ws://t1.homelab/ws node test/replay-flow-exclusion.test.js
//   (default: ws://127.0.0.1:7682/ws — a local `node server.js`)
const { spawn } = require('child_process');
const WebSocket = require('ws');
const URL = process.env.WEBTERM_URL || 'ws://127.0.0.1:7682/ws';
let sid = null;
let phase = 0;
let S = null;
const timeout = setTimeout(() => { console.error('FAIL: timeout phase=' + phase); process.exit(1); }, 120000);

// slow consumer, isolated process: throttles every binary frame at ~256 KiB/s
const SLOW_CLIENT = `
const WebSocket = require('ws');
const ws = new WebSocket(process.env.WEBTERM_URL);
let sawReplay = false;
ws.on('open', () => ws.send(JSON.stringify({ type: 'hello', id: process.env.SID, cols: 80, rows: 24 })));
ws.on('message', (d, bin) => {
  if (!bin) {
    const m = JSON.parse(d.toString());
    if (m.type === 'replay') sawReplay = true;
    else if (m.type === 'live' && process.send) process.send('live');
    return;
  }
  if (!sawReplay) return;
  const t = Date.now();
  while (Date.now() - t < d.length / 250) {} // ~1s per 256 KiB frame
  if (!ws.__sent) { ws.__sent = true; if (process.send) process.send('mid-replay'); }
});
`;

function conn() {
  const ws = new WebSocket(URL);
  ws.on('open', () => ws.send(JSON.stringify({ type: 'hello', id: sid, cols: 80, rows: 24 })));
  return ws;
}

// Phase 1: fast client F floods > 1 MiB and STAYS attached (idle, drained).
// F keeps a bounded rolling tail (never re-concats the full history — that
// would make F itself a slow client and stall the very flood it measures)
const F = conn();
let fTail = '';
let fBytes = 0;
F.on('message', (d, bin) => {
  if (!bin) {
    const m = JSON.parse(d.toString());
    if (m.type === 'init') {
      sid = m.id;
      phase = 1;
      F.send(JSON.stringify({ type: 'input', data: 'stty -echo\n' }));
      // ble.sh echoes the command line itself, so gate on reversed markers
      setTimeout(() => F.send(JSON.stringify({ type: 'input', data: 'echo BEGIN_RING; seq 1 200000; echo RING_DONE | rev\n' })), 500);
    }
    return;
  }
  fBytes += d.length;
  fTail = (fTail + d.toString('utf8')).slice(-65536);
  if (phase === 1 && fTail.includes('ENOD_GNIR')) {
    phase = 2;
    console.log('PASS: flood1 done (' + fBytes + ' bytes); F stays attached');
    setTimeout(spawnSlow, 800); // settle: F's queue fully flushed
  }
});

// Phase 2: slow consumer S (child process) re-attaches and drains slowly
function spawnSlow() {
  S = spawn(process.execPath, ['-e', SLOW_CLIENT], {
    env: { ...process.env, WEBTERM_URL: URL, SID: sid },
    stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
  });
  S.on('message', (m) => {
    if (m === 'mid-replay') { console.log('PASS: slow client mid-replay'); flood2(); }
    else if (m === 'live') console.log('PASS: slow client live marker (replay drained)');
  });
  S.on('error', () => {});
}

// Phase 3: F streams a moderate flood while S is mid-replay; must not stall
function flood2() {
  phase = 3;
  const t0 = Date.now();
  const marker = 'ENOD_2DOOLF'; // `echo FLOOD2_DONE | rev` output
  F.send(JSON.stringify({ type: 'input', data: 'echo BEGIN_F2; seq 1 120000; echo FLOOD2_DONE | rev\n' }));
  const iv = setInterval(() => {
    if (fTail.includes(marker)) {
      const lat = Date.now() - t0;
      const ok = lat < 3000;
      console.log('FLOOD2 LATENCY: ' + lat + 'ms (threshold 3000ms)');
      console.log(ok ? 'PASS: pty not paused behind slow replay client (replay bytes excluded from watermark)' : 'FAIL: pty paused behind slow replay client');
      clearInterval(iv);
      clearTimeout(timeout);
      F.send(JSON.stringify({ type: 'close' }));
      if (S) S.kill();
      setTimeout(() => process.exit(ok ? 0 : 1), 200);
    }
  }, 25);
  setTimeout(() => { // safety: flood2 never completed -> pty stalled behind replay
    clearInterval(iv);
    console.error('FAIL: flood2 never completed (pty paused behind slow replay client)');
    clearTimeout(timeout);
    if (S) S.kill();
    process.exit(1);
  }, 20000);
}
