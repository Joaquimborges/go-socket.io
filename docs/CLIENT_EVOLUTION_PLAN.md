# Fork Plan — Simple Socket.IO Client

Fork for a Go backend that needs a stable, minimal Socket.IO v4 client.

**Philosophy:** fewer files, less code, fewer abstractions.

**References:** [CLIENT_AUDIT.md](./CLIENT_AUDIT.md) · [README.md](../README.md)

---

## Current status (June 2026)

| Phase | Status |
|---|---|
| Remove server and legacy deps | ✅ Done |
| Minimal client (single `Client`, no namespaces/rooms/ACK) | ✅ Done |
| Engine.IO v4, PING→PONG, WebSocket-only | ✅ Done |
| Automatic reconnect with backoff | ✅ Done |
| HTTP headers on dial (`WithHeaders`) | ✅ Done |
| Integration + soak tests | ⏳ Pending |

**Next step:** integration tests against a real Socket.IO v4 server (`testdata/`) and local soak before production deploy.

---

## Goal

- Connect to Socket.IO v4 (Node.js)
- Stay connected for days
- Reconnect automatically (backoff 1s → … → cap 30s)
- Emit and receive JSON
- Respond to heartbeat (PING → PONG)
- Send HTTP headers on handshake (auth, etc.)

**No ACK** — the backend does not use response callbacks on any event.

---

## Public API

```go
client, err := socketio.NewClient(url,
    socketio.WithHeaders(http.Header{
        "Authorization": {"Bearer <token>"},
    }),
)

client.On("machine_connected", func(data MachineConnected) { ... })

client.OnConnect(func() { log.Println("connected") })       // every successful connection, including reconnect
client.OnDisconnect(func(err error) { log.Println("down", err) })

err := client.Connect()
if err := client.Emit("show_message", map[string]any{"username": "pedro"}); err != nil {
    // err == socketio.ErrNotConnected when offline
}
client.Close()
```

| Method | Description |
|---|---|
| `NewClient(url, opts...)` | Creates the client |
| `WithHeaders(h)` | HTTP headers on every dial (including reconnect) |
| `On(event, handler)` | Socket.IO events |
| `OnConnect()` | Connection established (first time and after each reconnect) |
| `OnDisconnect(err)` | Transport dropped, before backoff |
| `Connect()` | Starts loops + reconnect |
| `Emit(...)` | Sends JSON; returns `error` (`ErrNotConnected` when offline) |
| `Close()` | Stops reconnect and closes |

**Rejected:** separate `OnReconnect` (redundant with `OnConnect`).

**Rejected:** ACK / `Emit` with callback.

**Exported errors:** `ErrEmptyAddr`, `ErrNotConnected`, `ErrAlreadyConnected`.

---

## Architecture

```
App → Client (single exported struct) → engineio → websocket
              ↓
           parser
```

- **One `Client` struct** — private fields, loops in `client.go` + helpers in the same package.
- **No** separate `connection` struct (avoids pass-through facade).
- **`engineio/` + `parser/`** — subpackages kept (protocol already exists).

```
readLoop  → parser → On() handler (synchronous)
Emit      → writeChan → writeLoop → engineio
```

- **2 goroutines** (read + write). Fatal errors exit the readLoop.
- **`connectOnce()`** internal — reconnect calls this, not public `Connect()`.

---

## No ACK — what was removed

| Removed | Reason |
|---|---|
| `ack sync.Map`, ACK handlers | No response callbacks |
| `nextID()`, ACK branch in `Emit` | ACK IDs unnecessary |
| `parser.Ack` case in readLoop | Server ACK packets answered internally when `NeedAck` |

**Parser:** keeps `Ack` types in the decoder (protocol exists); the public API does not expose callbacks.

---

## Root namespace — validate, do not ignore

The client supports **only** the default namespace (`""` or `"/"` on the wire).

| `header.Namespace` | Action |
|---|---|
| `""` | OK — implicit root (`42[...]`) |
| `"/"` | OK |
| any other (`/admin`, `/foo`) | **Warn + discard** — do not dispatch handler |

Emit always uses the root namespace. If the server emits on another namespace, the log appears immediately.

---

## Cleanup principle (future)

```
integration (connect, emit, receive, reconnect)
    → soak
    → remove polling / buffer.go / dead code (optional)
```

---

## Phase history (completed)

The phases below document the fork evolution. The stable API is described in the [README](../README.md).

<details>
<summary>PR-1 — Remove server</summary>

- Removed `server.go`, Redis, UUID, `_examples/`, server-side code in `engineio/`
</details>

<details>
<summary>PR-2 — Minimal client</summary>

- Single `Client` struct; `On`, `OnConnect`, `OnDisconnect`, `Emit`, `Connect`, `Close`
- Removed namespaces, rooms, broadcast, ACK
</details>

<details>
<summary>PR-3 — v4 stability</summary>

- `EIO=4`, PING→PONG, WebSocket-only dial, idempotent `Connect()`
</details>

<details>
<summary>PR-4 — Reconnect</summary>

- Exponential backoff (1s → 30s), `Close()` cancels loop
</details>

<details>
<summary>Extra — WithHeaders</summary>

- `WithHeaders(http.Header)` option for auth and custom headers on handshake
</details>

---

## Pending

### Tests + soak

- `testdata/socketio-v4-server/` — minimal Node server for CI
- Integration: connect, emit, receive, reconnect (no ack)
- 4h local soak; 24h recommended before production deploy

**Optional (hygiene):** remove `polling/`, `payload/`, `parser/buffer.go` after soak OK.

---

## Definition of Done

- [x] No server code
- [x] No ACK in the public API (emit/receive)
- [x] API: `NewClient`, `Connect`, `Emit`, `On`, `OnConnect`, `OnDisconnect`, `Close`
- [x] WebSocket + EIO=4 + PING→PONG
- [x] Automatic reconnect
- [x] HTTP headers on dial
- [ ] Integration tests (no ack)
- [ ] Stable soak
- [x] Clean `go test ./... -race`

---

## Out of scope (remains)

FSM, extensive options beyond `WithHeaders`, Context in the API, async handlers, rooms, broadcast, server, polling upgrade, custom namespaces.

---

**Development workflow:** see [DEVELOPMENT.md](./DEVELOPMENT.md).
