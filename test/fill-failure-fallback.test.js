'use strict';
// Regression: a synchronous throw while the hidden fill terminal parses the
// ring must NOT strand the visible terminal on the preview suffix (the
// "scrollback collapsed to ~500 lines, refresh doesn't help" bug) with live
// output parked forever. The client must fall back to replaying the retained
// ring into the visible terminal: full history, correct final screen, and
// post-live frames delivered — no parked frames.
//
// Runs the REAL index.html client script in a sandbox with a fake
// Terminal/WebSocket, injects a throw into the fill parse (scenario A) and
// into the swap open (scenario B), and asserts the visible terminal ends up
// with the full ring + live frames.
//
// Standalone (no server needed):
//   node test/fill-failure-fallback.test.js
const fs = require('fs');
const path = require('path');
const vm = require('vm');

const html = fs.readFileSync(path.join(__dirname, '..', 'public', 'index.html'), 'utf8');
const m = html.match(/<script>([\s\S]*?)<\/script>\s*<\/body>/);
if (!m) { console.error('FAIL: client script not found in index.html'); process.exit(1); }

// --- fakes ---------------------------------------------------------------
class FakeTerminal {
  constructor(opts) {
    this.opts = opts || {};
    this.cols = this.opts.cols || 80;
    this.rows = this.opts.rows || 24;
    this.text = '';
    this.lines = 0;
    this.disposed = false;
    this.throwMarker = null; // scenario A: bytes that make write() throw
    this.openThrows = false; // scenario B: open() throws at swap time
    this.id = FakeTerminal.count++;
    FakeTerminal.instances.push(this);
  }
  loadAddon() {}
  open() { if (this.openThrows) throw new Error('simulated swap open failure'); }
  focus() {}
  dispose() { this.disposed = true; }
  reset() { this.text = ''; this.lines = 0; }
  scrollToBottom() {}
  getSelection() { return ''; }
  attachCustomKeyEventHandler() {}
  onData() {}
  onResize() {}
  onSelectionChange() {}
  get unicode() { return { activeVersion: '11' }; }
  set unicode(_) {}
  write(data, cb) {
    const s = Buffer.from(data).toString('utf8');
    if (this.throwMarker && s.includes(this.throwMarker)) {
      throw new Error('simulated fill parse failure');
    }
    this.text += s;
    this.lines = this.text.split('\n').length - 1;
    if (cb) cb();
  }
}
FakeTerminal.count = 0;
FakeTerminal.instances = [];

function AddonBase() {}
AddonBase.prototype.loadAddon = function () {};
class WebglStub { onContextLoss() {} dispose() {} }
class FitStub { fit() {} }
class SearchStub { findNext() {} }

class FakeWS {
  constructor(url) {
    this.url = url;
    this.readyState = 1;
    this.binaryType = '';
    this.sent = [];
    FakeWS.instance = this;
  }
  send(data) { this.sent.push(data); }
  close() {}
}
FakeWS.OPEN = 1;

function makeSandbox(storedSid) {
  const sandbox = {
    console,
    Uint8Array,
    ArrayBuffer,
    JSON,
    Promise,
    setTimeout,
    clearTimeout,
    Math,
    localStorage: {
      getItem: () => storedSid || '',
      setItem: () => {},
    },
    navigator: { clipboard: { writeText: () => Promise.resolve() } },
    document: { getElementById: () => ({}) },
    location: { protocol: 'http:', host: 'sandbox' },
    addEventListener: () => {},
    window: null, // self-reference below
    Terminal: FakeTerminal,
    WebSocket: FakeWS,
    FitAddon: { FitAddon: FitStub },
    ClipboardAddon: { ClipboardAddon: AddonBase },
    WebLinksAddon: { WebLinksAddon: AddonBase },
    Unicode11Addon: { Unicode11Addon: AddonBase },
    UnicodeGraphemesAddon: { UnicodeGraphemesAddon: AddonBase },
    ImageAddon: { ImageAddon: AddonBase },
    SearchAddon: { SearchAddon: SearchStub },
    CanvasAddon: { CanvasAddon: AddonBase },
    WebglAddon: { WebglAddon: WebglStub },
  };
  sandbox.window = sandbox;
  return sandbox;
}

// --- frame construction ---------------------------------------------------
function lines(from, to, marker) {
  let out = '';
  for (let i = from; i < to; i++) out += (marker && i % 500 === 0 ? marker : '') + 'L' + String(i).padStart(5, '0') + '\n';
  return Buffer.from(out, 'utf8');
}

// Run the real client, drive an attach with a failing fill, return the visible
// terminal + collected state for assertions.
function runAttach(failMode) {
  FakeTerminal.count = 0;
  FakeTerminal.instances = [];
  FakeWS.instance = null;
  const sandbox = makeSandbox('');
  vm.createContext(sandbox);
  vm.runInContext(m[1], sandbox, { filename: 'index.html' });
  const ws = FakeWS.instance;
  if (!ws) { console.error('FAIL: client never connected'); process.exit(1); }
  const visible = FakeTerminal.instances[0];
  // visible terminal created before connect(); the scratch appears on 'fill'
  ws.onopen();
  const hello = ws.sent.find((s) => typeof s === 'string' && s.includes('"hello"'));
  if (!hello) { console.error('FAIL: no hello after open'); process.exit(1); }

  const frame = (d) => ws.onmessage({ data: d });
  // init -> replay -> suffix -> fill -> ring frames -> live -> post-live frame
  frame(JSON.stringify({ type: 'init', id: 'sess1', replay: true }));
  frame(JSON.stringify({ type: 'replay' }));
  frame(lines(29400, 30000)); // preview suffix (what the visible terminal gets first)
  frame(JSON.stringify({ type: 'fill' }));
  const scratch = FakeTerminal.instances[1];
  if (!scratch) { console.error('FAIL: fill never created the hidden terminal'); process.exit(1); }
  if (failMode === 'fillParse') scratch.throwMarker = 'ZOMBIE';
  if (failMode === 'swapOpen') scratch.openThrows = true;
  frame(lines(0, 10000));                    // ring 1
  frame(lines(10000, 20000, 'ZOMBIE'));      // ring 2 (carries the throwing bytes in fillParse mode)
  frame(lines(20000, 30000));                // ring 3
  frame(JSON.stringify({ type: 'live' }));
  frame(Buffer.from('AFTER_LIVE\n', 'utf8')); // post-live: must NOT be parked
  return { visible, scratch, ws, term: sandbox.window.term };
}

function checkScenario(name, failMode) {
  const { visible, scratch, term } = runAttach(failMode);
  const ok = [];
  ok.push(['full ring replayed into the visible terminal', visible.lines >= 30001]);
  ok.push(['ring start (oldest history) present', visible.text.includes('L00000\n')]);
  ok.push(['final screen correct (ring tail)', visible.text.endsWith('L29999\nAFTER_LIVE\n')]);
  ok.push(['post-live frame delivered (not parked)', visible.text.includes('AFTER_LIVE\n')]);
  ok.push(['failed scratch disposed', scratch.disposed]);
  ok.push(['no swap: visible terminal still the live one', term === visible]);
  const failed = ok.filter(([, pass]) => !pass);
  console.log(`--- ${name} ---`);
  for (const [desc, pass] of ok) console.log((pass ? 'PASS' : 'FAIL') + ': ' + desc);
  if (failed.length) { console.error(`FAIL: ${name} — ${failed.length} assertion(s)`); process.exit(1); }
  console.log(`PASS: ${name}`);
}

checkScenario('fill parse failure falls back to visible terminal', 'fillParse');
checkScenario('swap open failure falls back to visible terminal', 'swapOpen');
console.log('PASS: all fallback scenarios');
process.exit(0);
