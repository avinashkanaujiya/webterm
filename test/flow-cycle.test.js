'use strict';
// flow-control cycle test: throttle consumption hard (server queue exceeds the
// watermark -> pty pauses), then unthrottle (queue drains -> pty resumes).
// Verifies the per-session tick heals the pause without relying on ws cbs.
const WebSocket = require('ws');
const ws = new WebSocket(process.env.WEBTERM_URL || 'ws://127.0.0.1:7682/ws');
let raw = '';
let frames = 0;
let throttled = true;
let phase = 'flood';
const timeout = setTimeout(() => {
  console.error('FAIL: timeout phase=' + phase + ' frames=' + frames + ' tail=' + JSON.stringify(raw.slice(-60)));
  process.exit(1);
}, 90000);

ws.on('open', () => {
  ws.send(JSON.stringify({ type: 'hello', cols: 80, rows: 24 }));
  setTimeout(() => ws.send(JSON.stringify({ type: 'input', data: 'stty -echo\n' })), 2000);
  setTimeout(() => ws.send(JSON.stringify({ type: 'input', data: 'echo BEGIN_FC; seq 1 1000000\n' })), 2600);
});
ws.on('message', (data, isBinary) => {
  if (!isBinary) return;
  frames++;
  const s = data.toString('utf8');
  raw += s;

  if (throttled && raw.length > 1500000) {
    throttled = false;
    phase = 'drain';
    console.log('unthrottled at ' + raw.length + 'B, waiting for server to resume pty');
  }
  if (phase === 'flood' && throttled) {
    // hard throttle: burn ~8ms per frame -> client falls > 1MB behind
    const t = Date.now();
    while (Date.now() - t < 8) { /* burn */ }
  }

  if (raw.includes('BEGIN_FC') && raw.includes('1000000\r\n') && raw.includes('root@mail')) {
    phase = 'done';
    const tail = raw.slice(raw.indexOf('BEGIN_FC'));
    const nums = tail.split('\n').filter((l) => /^\d+$/.test(l.trim())).map(Number);
    const ok = nums.length === 1000000 && nums[0] === 1 && nums[999999] === 1000000;
    console.log('RESULT: frames=' + frames + ' bytes=' + raw.length + ' nums=' + nums.length + (ok ? ' CONTIGUOUS' : ' GAPS'));
    clearTimeout(timeout);
    process.exit(ok ? 0 : 1);
  }
});
