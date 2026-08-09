# webterm

A VS Code-style web terminal: real PTY sessions that **survive browser refreshes**, with browser-native copy-paste and pty-level flow control. Built on [xterm.js](https://github.com/xtermjs/xterm.js) (client) + [node-pty](https://github.com/microsoft/node-pty) (server) over a WebSocket.

The architecture mirrors VS Code's terminal: the browser runs a full terminal emulator (buffer, rendering, selection, clipboard stay local), while the server holds real shell processes in a persistent host process. Closing/refreshing the tab kills nothing — the shell keeps running and reconnects re-attach to the same session.

## Features

- **Persistent sessions** — the PTY lives in the server process, decoupled from any browser connection. Refresh, close the tab, or lose the network and reconnect: same shell, same history.
- **Screen restore on re-attach** — recent output is replayed to a fresh client (`replay`/`live` protocol markers), so the visible screen and scrollback come back after a refresh, including full-screen TUIs.
- **Native copy-paste** — selection and clipboard are browser operations; no tmux in the path, so no nested emulator and no tmux mouse capture.
- **Zero-loss flow control** — output is queued per client with bounded watermarks (1 MiB high / 256 KiB low); when a client falls behind, the PTY itself is paused (the shell blocks on the pty buffer), then resumed when it catches up. Nothing is dropped (verified with a 1,000,000-line flood through a throttled client).
- **Sixel images** — `lsix` / `img2sixel` output renders inline.
- **Multiple clients per session** — output broadcasts to every attached client.
- **No tmux required** — persistence is handled by the server process.

## Keyboard shortcuts

| Shortcut | Action |
|---|---|
| select text (mouse) | copies to the OS clipboard automatically |
| `Ctrl+V` | paste (browser-native; right-click paste works too) |
| `Ctrl+Shift+F` | find (prompt + next match) |
| `Ctrl+=` / `Ctrl+-` | increase / decrease font size |
| type `exit` | end the shell; **reload the page** to start a fresh session |
| `Ctrl+C` (with a selection) | the selection was already copied on select; otherwise sends SIGINT |

There is no "new session" button by design: `exit` + reload gives you a fresh shell, while ordinary refreshes keep the same session.

## Usage

1. Open the terminal URL (e.g. `https://t1.homelab`).
2. Type — input goes straight to the shell.
3. Refresh the page whenever you like: the shell, its environment, and its running jobs survive; the screen is restored from the server's output replay.
4. To start over: type `exit`, then reload.

Session identity is kept in `localStorage`; the server creates a new session when the stored id no longer exists (e.g. after a server restart).

## Deployment

Source and compose live in this repo.

```bash
# build + push the image (uses homelab-build conventions; local registry box.reg)
homelab-build webterm

# deploy the t1..t9 fleet
docker compose pull
docker compose up -d
```

Each container is an independent terminal instance (`t1`…`t9`), each with its own in-memory session space. Environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `PORT` | `7682` | HTTP/WebSocket listen port |
| `SHELL_CMD` | `/bin/bash -l` | command the PTY spawns (e.g. an `nsenter` host-root shell) |
| `TITLE` | `webterm` | browser tab title, injected into the page (e.g. `t1-box`) |

The included compose runs privileged containers with `pid: host` and an `nsenter` host-root shell — the same trust posture as the ttyd fleet it complements. **Sessions are in-memory**: a container restart (or host reboot) starts a fresh shell; a browser refresh does not.

## Architecture

```
Browser (xterm.js)                      Server (Node)
┌───────────────────────┐               ┌──────────────────────────────┐
│ buffer / render /     │  text frames  │ SessionManager: id → PTY     │
│ selection / clipboard │ ◄───────────► │  per-client output queues    │
│ (all local)           │  binary out   │  + replay ring (1 MiB)       │
└───────────────────────┘               └──────────────────────────────┘
```

- **Protocol** — text frames are JSON control (`hello`, `input`, `resize`, `close`, `init`, `replay`, `live`, `exit`); binary frames are raw terminal output. Control frames go through the same per-client queue as output, so `live`/`exit` can never overtake pending bytes.
- **Flow control** — the server hands output to each client from a bounded queue (1 MiB high watermark); when any client's outstanding bytes exceed the watermark the PTY is paused and a recheck timer runs until it drains (256 KiB low watermark resumes it). `perMessageDeflate` is off and `TCP_NODELAY` on — terminal streams are latency-sensitive.
- **Rendering** — mirrors the ttyd client exactly: WebGL renderer with canvas fallback, Unicode `15-graphemes` width rules (emoji/ZWI aware), fontSize 13, font stack `Consolas,Liberation Mono,Menlo,Courier,monospace`. Deviating from this font/size stack changes cell metrics and misaligns box-drawing borders by a column.

## Testing

Headless tests in `test/` run against any live instance:

```bash
WEBTERM_URL=ws://t1.homelab/ws node test/session-persistence.test.js   # session survives disconnect; same shell
WEBTERM_URL=ws://t1.homelab/ws node test/flood-flow-control.test.js    # 1M-line flood, zero drops, no deadlock
WEBTERM_URL=ws://t1.homelab/ws node test/replay-ordering.test.js       # init→replay→live→exit ordering
```

Defaults to `ws://127.0.0.1:7682/ws` for a local `node server.js`.

## Notes & gotchas

- `node-pty` needs a C++ toolchain (`python3 make g++`) to build in the container.
- xterm.js addon UMD bundles export **namespace objects**: `window.FitAddon.FitAddon`, `window.WebLinksAddon.WebLinksAddon`, `window.UnicodeGraphemesAddon.UnicodeGraphemesAddon` (plural), etc.
- The graphemes addon registers its unicode version as `15-graphemes` (not `graphemes`); `Unicode11Addon` requires `allowProposedApi: true`.
- `@xterm/addon-zoom` does not exist — font zoom is implemented at app level.
- `@xterm/addon-ligatures` is ESM-only (`.mjs`) and needs a ligature-capable font; not bundled.
- No authentication is built in — put the service behind your usual gateway (this homelab's deployment is tailnet-only behind Caddy).
