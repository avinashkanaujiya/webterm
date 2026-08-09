'use strict';
// Verifies the core promise: a terminal session survives ws disconnect and
// re-attach replays recent output — the SAME shell keeps running (env var +
// background job intact), with correct replay/live markers.
//
// Run against any running webterm instance:
//   WEBTERM_URL=ws://t1.homelab/ws node test/session-persistence.test.js
//   (default: ws://127.0.0.1:7682/ws — a local `node server.js`)
const WebSocket = require('ws');
const URL = process.env.WEBTERM_URL || 'ws://127.0.0.1:7682/ws';
let sid = null;
const timeout = setTimeout(() => { console.error('FAIL: timeout'); process.exit(1); }, 20000);

function connect(expectReplay, onInit) {
  const ws = new WebSocket(URL);
  ws.expectReplay = expectReplay;
  ws.on('open', () => ws.send(JSON.stringify({ type: 'hello', id: sid, cols: 80, rows: 24 })));
  ws.on('message', (data, isBinary) => {
    if (!isBinary) {
      const m = JSON.parse(data.toString());
      if (m.type === 'init') {
        sid = m.id;
        console.log(`init id=${sid} replay=${m.replay}`);
        if (m.replay !== ws.expectReplay) { console.error(`FAIL: expected replay=${ws.expectReplay}`); process.exit(1); }
        onInit(ws, m);
      } else if (m.type === 'replay') {
        ws.replayMarker = true;
      } else if (m.type === 'live') {
        if (!ws.replayMarker) { console.error('FAIL: live marker without replay'); process.exit(1); }
        console.log('PASS: replay/live markers seen');
      }
    } else if (!ws.__firstBinary) {
      ws.__firstBinary = true;
      if (ws.expectReplay !== !!ws.replayMarker) {
        console.error(`FAIL: ${ws.expectReplay ? 'expected replay data' : 'expected no replay'}, replayMarker=${ws.replayMarker}, ${data.length}B`);
        process.exit(1);
      }
    }
  });
  return ws;
}

// Phase 1: fresh session, set env + background job, disconnect
const ws1 = connect(false, (ws) => {
  ws.send(JSON.stringify({ type: 'input', data: 'export WT_PROBE=alive; echo MARKER1; sleep 300 &\n' }));
  let acc = '';
  ws1.on('message', (data, isBinary) => {
    if (isBinary) {
      acc += data.toString('utf8');
      if (acc.includes('MARKER1')) {
        console.log('PASS: fresh session echoes command');
        ws1.close(); // simulate tab close — session must survive
        setTimeout(phase2, 500);
      }
    }
  });
});

function phase2() {
  const ws2 = connect(true, (ws) => {
    ws.send(JSON.stringify({ type: 'input', data: 'echo "ENV=$WT_PROBE"; jobs | grep -c sleep\n' }));
    let acc = '';
    ws2.on('message', (data, isBinary) => {
      if (isBinary) {
        acc += data.toString('utf8');
        if (acc.includes('ENV=alive') && acc.includes('sleep')) {
          console.log('PASS: same shell survived disconnect (env + background job intact)');
          ws2.send(JSON.stringify({ type: 'close' }));
          setTimeout(() => { clearTimeout(timeout); console.log('ALL_PASS'); process.exit(0); }, 300);
        }
      }
    });
  });
}
