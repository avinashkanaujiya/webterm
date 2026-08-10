'use strict';
// Regression: re-attach after the replay ring has filled to its 1 MiB cap must
// still deliver init -> replay -> binary -> live. Guard against the pump
// deadlock where a single replay frame >= Q_HIGH can never pass the
// `outstanding < Q_HIGH` gate (the queue wedged and the pty got paused
// forever -> blank screen on refresh after a long session).
//
// The shell runs ble.sh, so mirror flood-flow-control.test.js: stty -echo
// first, gate on output markers only (never the echoed command line), and
// generate the flood with seq (known to survive the line editor).
//
// Run against any running webterm instance:
//   WEBTERM_URL=ws://t1.homelab/ws node test/replay-full-ring.test.js
//   (default: ws://127.0.0.1:7682/ws — a local `node server.js`)
const WebSocket = require('ws');
const URL = process.env.WEBTERM_URL || 'ws://127.0.0.1:7682/ws';
let sid = null;
let phase = 0;
const timeout = setTimeout(() => { console.error('FAIL: timeout phase=' + phase); process.exit(1); }, 120000);

function conn() {
  const ws = new WebSocket(URL);
  ws.on('open', () => ws.send(JSON.stringify({ type: 'hello', id: sid, cols: 80, rows: 24 })));
  return ws;
}

// Phase 1: fresh session, flood ~1.4 MB of output so the ring hits its cap
const a = conn();
const aChunks = [];
a.on('message', (d, bin) => {
  if (!bin) {
    const m = JSON.parse(d.toString());
    if (m.type === 'init') {
      sid = m.id;
      phase = 1;
      a.send(JSON.stringify({ type: 'input', data: 'stty -echo\n' }));
      // ble.sh echoes the command line (its own renderer, stty -echo doesn't
      // stop it), so gate on a marker that cannot appear in that echo:
      // `echo RING_DONE | rev` outputs ENOD_GNIR, never printed verbatim
      setTimeout(() => a.send(JSON.stringify({ type: 'input', data: 'echo BEGIN_RING; seq 1 200000; echo RING_DONE | rev\n' })), 500);
    }
    return;
  }
  aChunks.push(d);
  const acc = Buffer.concat(aChunks).toString('utf8');
  if (!a.__done && acc.includes('ENOD_GNIR')) {
    a.__done = true;
    console.log('PASS: flood done (' + acc.length + ' bytes), closing (tab close)');
    a.close();
    setTimeout(phase2, 500);
  }
});

// Phase 2: re-attach (the refresh path). Replay must arrive, be ~1 MiB, and
// contain the tail of the flood; live output must flow after.
function phase2() {
  const b = conn();
  const seen = [];
  let replayBytes = 0;
  const replayChunks = [];
  let liveStarted = false;
  let postLive = '';
  let done = false;
  b.on('message', (d, bin) => {
    if (!bin) {
      const m = JSON.parse(d.toString());
      if (m.type === 'init') {
        phase = 2;
        if (m.replay !== true) { console.error('FAIL: expected replay on re-attach'); process.exit(1); }
        seen.push('init(replay=true)');
      } else if (m.type === 'replay') { seen.push('replay-marker'); }
      else if (m.type === 'live') {
        seen.push('live-marker');
        liveStarted = true;
        if (!done) {
          done = true;
          b.send(JSON.stringify({ type: 'input', data: 'echo AFTER_RING\n' }));
          setTimeout(check, 2000);
        }
      }
      return;
    }
    if (!liveStarted) { replayBytes += d.length; replayChunks.push(d); }
    else postLive += d.toString('utf8');
  });
  function check() {
    const acc = Buffer.concat(replayChunks).toString('utf8');
    const order = seen.join('>');
    const ok = order === 'init(replay=true)>replay-marker>live-marker'
      && replayBytes >= 1000000
      && acc.includes('ENOD_GNIR')
      && acc.includes('200000')
      && postLive.includes('AFTER_RING');
    console.log('ORDER: ' + order + ' replayBytes=' + replayBytes
      + ' ringTailHasMarker=' + acc.includes('ENOD_GNIR')
      + ' ringTailHas200000=' + acc.includes('200000')
      + ' shellAlive=' + postLive.includes('AFTER_RING'));
    console.log(ok ? 'PASS: full-ring replay delivered, shell alive after' : 'FAIL: full-ring re-attach broken');
    clearTimeout(timeout);
    b.send(JSON.stringify({ type: 'close' }));
    process.exit(ok ? 0 : 1);
  }
}
