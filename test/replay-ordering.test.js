'use strict';
// Verifies frame ordering on re-attach: init(replay=true) -> replay-marker ->
// binary replay -> live-marker -> (live output) -> exit, with NO binary frames
// after the exit message. Guards against control frames overtaking queued
// terminal output (the ordered-queue invariant).
//
// Run against any running webterm instance:
//   WEBTERM_URL=ws://t1.homelab/ws node test/replay-ordering.test.js
//   (default: ws://127.0.0.1:7682/ws — a local `node server.js`)
const WebSocket = require('ws');
const URL = process.env.WEBTERM_URL || 'ws://127.0.0.1:7682/ws';
let sid = null;
let phase = 0;
const timeout = setTimeout(() => { console.error('FAIL: timeout phase=' + phase); process.exit(1); }, 30000);

function conn() {
  const ws = new WebSocket(URL);
  ws.on('open', () => ws.send(JSON.stringify({ type: 'hello', id: sid, cols: 80, rows: 24 })));
  return ws;
}

// Phase 1: create session, emit ~400KB of output, close
const a = conn();
let aOut = 0;
a.on('message', (d, b) => {
  if (!b) {
    const m = JSON.parse(d.toString());
    if (m.type === 'init') {
      sid = m.id;
      phase = 1;
      a.send(JSON.stringify({ type: 'input', data: 'seq 1 60000\n' }));
    }
    return;
  }
  aOut += d.length;
  if (aOut > 200000) { a.close(); setTimeout(phase2, 300); }
});

function phase2() {
  const b = conn();
  const seen = [];
  let replayBytes = 0;
  let binAfterExit = 0;
  let done = false;
  b.on('message', (d, bin) => {
    const s = bin ? '[BIN ' + d.length + ']' : d.toString();
    if (!bin) {
      const m = JSON.parse(s);
      if (m.type === 'init') {
        phase = 2;
        if (m.replay !== true) { console.error('FAIL: expected replay on re-attach'); process.exit(1); }
        seen.push('init(replay=true)');
      } else if (m.type === 'replay') { seen.push('replay-marker'); }
      else if (m.type === 'live') {
        seen.push('live-marker');
        b.send(JSON.stringify({ type: 'input', data: 'exit\n' }));
      }
      else if (m.type === 'exit' && !done) {
        done = true;
        seen.push('exit');
        // drain window: anything queued after the exit frame must show up now
        setTimeout(() => {
          const order = seen.join('>');
          const ok = order === 'init(replay=true)>replay-marker>live-marker>exit' && replayBytes > 200000 && binAfterExit === 0;
          console.log('ORDER: ' + order + ' replayBytes=' + replayBytes + ' binAfterExit=' + binAfterExit);
          console.log(ok ? 'PASS: replay before live, live before exit, no binary after exit' : 'FAIL: ordering violation');
          clearTimeout(timeout);
          process.exit(ok ? 0 : 1);
        }, 800);
      }
      return;
    }
    if (phase === 2 && seen.includes('replay-marker') && !seen.includes('live-marker')) replayBytes += d.length;
    else if (seen.includes('exit')) binAfterExit += d.length;
  });
}
