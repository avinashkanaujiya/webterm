'use strict';
// Verifies flow control under a 1M-line flood with a slow consumer (busy-wait
// throttle keeps the ws receiver healthy while forcing the server queue past
// its watermark -> PTY pauses, then resumes). Invariants:
//   - ALL lines 1..1000000 arrive contiguously (no drops)
//   - the shell is still responsive afterwards (no flow deadlock)
//
// Run against any running webterm instance:
//   WEBTERM_URL=ws://t1.homelab/ws node test/flood-flow-control.test.js
//   (default: ws://127.0.0.1:7682/ws — a local `node server.js`)
const WebSocket = require('ws');
const ws = new WebSocket(process.env.WEBTERM_URL || 'ws://127.0.0.1:7682/ws');
const raw = [];
let rawLen = 0;
let frames = 0;
let begun = false;
let tail = '';
let phase = 'idle';
const timeout = setTimeout(() => {
  console.error('FAIL: timeout phase=' + phase + ' bytes=' + rawLen + ' frames=' + frames + ' tail=' + JSON.stringify(tail.slice(-60)));
  process.exit(1);
}, 120000);

ws.on('open', () => {
  ws.send(JSON.stringify({ type: 'hello', cols: 80, rows: 24 }));
  // wait out the host bash startup probe, then disable echo
  setTimeout(() => ws.send(JSON.stringify({ type: 'input', data: 'stty -echo\n' })), 2000);
  setTimeout(() => ws.send(JSON.stringify({ type: 'input', data: 'echo BEGIN_SEQ; seq 1 1000000\n' })), 2600);
});
ws.on('message', (data, isBinary) => {
  if (!isBinary) return;
  frames++;
  const s = data.toString('utf8');
  raw.push(s);
  rawLen += data.length;
  tail = (tail + s).slice(-200);
  begun = begun || s.includes('BEGIN_SEQ');
  // slow consumer: ~1ms busy-wait per frame -> client falls behind, server
  // queue grows past Q_HIGH, pty pauses, then resumes with zero loss
  const t = Date.now();
  while (Date.now() - t < 1) { /* burn */ }

  if (phase !== 'done' && rawLen > 100000 && begun && tail.includes('1000000\r\n')) {
    phase = 'done';
    const full = raw.join(''); // single O(n) pass, only after completion
    const nums = full.slice(full.indexOf('BEGIN_SEQ')).split('\n').filter((l) => /^\d+$/.test(l.trim())).map(Number);
    const ok = nums.length === 1000000 && nums[0] === 1 && nums[999999] === 1000000;
    console.log('RESULT: frames=' + frames + ' bytes=' + rawLen + ' nums=' + nums.length + ' first=' + nums[0] + ' last=' + nums[nums.length - 1]);
    if (!ok) { console.error('FAIL: missing/extra lines'); process.exit(1); }
    console.log('PASS: all 1..1000000 received contiguously through flow control');
    ws.send(JSON.stringify({ type: 'input', data: 'echo AFTER_FLOOD\n' }));
  }
  if (phase === 'done' && tail.includes('AFTER_FLOOD')) {
    clearTimeout(timeout);
    console.log('PASS: shell responsive after flood (no deadlock)');
    process.exit(0);
  }
});
ws.on('close', () => { console.error('FAIL: ws closed early'); process.exit(1); });
