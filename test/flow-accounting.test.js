'use strict';
// Unit test for the flow watermark accounting: replay bytes in flight
// (replayPending) are subtracted from a client's outstanding before the
// watermark check, so a slow replay catch-up never counts as live backlog,
// while live output stays bounded at Q_HIGH. Deterministic — pure function,
// no sockets, no timing. The end-to-end equivalent (slow replay client +
// concurrent flood) is unobservable on loopback because the kernel absorbs
// the whole replay; this is the precise guard for that accounting.
//
//   node test/flow-accounting.test.js   (from the repo root)
const path = require('path');
const { flowDecision, Q_HIGH, Q_LOW } = require(path.join(__dirname, '..', 'server.js'));
const MB = 1024 * 1024;

function client(raw, replayPending) {
  return { qBytes: raw, bufferedAmount: 0, _socket: { writableLength: 0 }, replayPending };
}

let fails = 0;
function check(name, cond) {
  console.log((cond ? 'PASS' : 'FAIL') + ': ' + name);
  if (!cond) fails++;
}

// replay in flight: 1.5 MiB raw outstanding, 1 MiB of it is replay -> live
// 0.5 MiB, under Q_HIGH -> no pause (the whole point of the subtraction)
check('replay in flight does not count toward watermark', !flowDecision([client(1.5 * MB, 1 * MB)]).anyOver);

// replay fully flushed (replayPending 0): live = raw -> 1.5 MiB is over
check('live backlog above watermark pauses', flowDecision([client(1.5 * MB, 0)]).anyOver);

// boundaries: exactly Q_HIGH is not over (>), one byte over is
check('outstanding exactly at watermark does not pause', !flowDecision([client(Q_HIGH, 0)]).anyOver);
check('outstanding one byte above watermark pauses', flowDecision([client(Q_HIGH + 1, 0)]).anyOver);

// live at Q_LOW or below -> allLow (resume condition); above -> not
check('at low watermark allLow', flowDecision([client(Q_LOW, 0)]).allLow);
check('above low watermark not allLow', !flowDecision([client(Q_LOW + 1, 0)]).allLow);

// multiple clients: any over -> pause; all under low -> resume
check('any client over pauses', flowDecision([client(0, 0), client(Q_HIGH + 1, 0)]).anyOver);
check('all clients under low resumes', flowDecision([client(1, 0), client(2, 0)]).allLow);
// replaying client contributes nothing; fast client decides
check('fast client decides when peer replaying', !flowDecision([client(0, 0), client(1.5 * MB, 1 * MB)]).anyOver);

console.log(fails === 0 ? 'ALL_PASS' : fails + ' FAILURES');
process.exit(fails === 0 ? 0 : 1);
