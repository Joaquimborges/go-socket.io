# Fork Plan — Simple Socket.IO Client

Fork for Go applications that need a stable, minimal Socket.IO v4 client: the `claw-machine-api` backend
today, and a Go client for the physical machine next.

**Philosophy:** fewer files, less code, fewer abstractions.

**References:** [CLIENT_AUDIT.md](./CLIENT_AUDIT.md) · [README.md](../README.md) · [ARCHITECTURE.md](./ARCHITECTURE.md)

---

## Current status (October 2026)

| Phase | Status |
|---|---|
| Remove server and legacy deps | ✅ Done |
| Minimal client (single `Client`, root namespace only) | ✅ Done |
| Engine.IO v4, PING→PONG, WebSocket-only | ✅ Done |
| Automatic reconnect with backoff | ✅ Done |
| HTTP headers on dial (`WithHeaders`) | ✅ Done |
| Phase 0 — per-session state, synchronous handshake, `CONNECT_ERROR` | ✅ Done (v1.1.0) |
| Phase 1 — `Reconnect`, `WithReconnectBackoff` with jitter, `WithEventErrorHandler` | ✅ Done (v1.1.0) |
| Integration tests (in-process server) and soak against the relay | ✅ Done — see [TESTING.md](./TESTING.md) |
| Integration tests against a real Node server in CI | ⏳ Pending |
| Phases 2–4 — explicit ack, `EmitWithAck`, namespaces | 💤 Deferred until a consumer needs them |

---

## Goal

- Connect to Socket.IO v4 (Node.js)
- Stay connected for days
- Reconnect automatically, with a backoff the application can tune
- Emit and receive JSON
- Respond to heartbeat (PING → PONG)
- Send HTTP headers on handshake (auth, etc.)
- Acknowledge server events with an empty receipt once the handler returns

---

## Public API

```go
client, err := socketio.NewClient(url,
    socketio.WithHeaders(http.Header{"X-Client-Key": {"<key>"}}),
    socketio.WithReconnectBackoff(200*time.Millisecond, 2*time.Second, 0.2), // optional
    socketio.WithEventErrorHandler(func(event string, err error) { ... }),  // optional
)

client.On("machine_connected", func(data MachineConnected) { ... })

client.OnConnect(func() { log.Println("connected") })       // every accepted connection, including reconnects
client.OnDisconnect(func(err error) { log.Println("down", err) })
client.OnReconnectAttempt(func(attempt int, wait time.Duration, err error) { ... }) // *ConnectError on refusal

err = client.Connect()
if err := client.Emit("show_message", map[string]any{"username": "pedro"}); err != nil {
    // err == socketio.ErrNotConnected when offline
}
_ = client.Reconnect() // drop the session and connect again, e.g. from a heartbeat watchdog
client.Close()
```

The full reference is in the [README](../README.md).

**Rejected:** separate `OnReconnect` (redundant with `OnConnect`).

**Exported errors:** `ErrEmptyAddr`, `ErrInvalidOption`, `ErrNotConnected`, `ErrAlreadyConnected`,
`ErrReconnectRequested`, `*ConnectError`.

---

## Architecture

```
App → Client (single exported struct) → engineio → websocket
              ↓
           parser
```

- **One `Client` struct**, plus one internal `session` per accepted connection that carries everything its
  loops use. See [ARCHITECTURE.md](./ARCHITECTURE.md).
- **`engineio/` + `parser/`** — subpackages kept (protocol already exists).

```
readLoop  → parser → On() handler (synchronous, in order)
Emit      → session.writeChan → writeLoop → engineio
```

- **2 goroutines per session** (read + write) plus `run`. Fatal errors end the session, and `run` reconnects.
- **`connectOnce()`** internal — dials, then waits for the server's `CONNECT` before the session exists.

---

## Acknowledgements — receipt only, for now

The first refactor removed ACK from the public API. Production then needed exactly one kind: the relay emits to
the backend with `timeout(ms).emitWithAck` and treats the empty ack as a receipt, redelivering to another
instance when it is missing. That receipt is what the client sends today:

| Situation | Ack |
|---|---|
| Handler returns | Empty ack after it returns |
| No handler | Empty ack at once |
| Handler panics or payload does not decode | No ack and the session ends, unless `WithEventErrorHandler` is set |
| Session ended meanwhile | Dropped |

**Not rejected, deferred.** Acks with data (Phase 2: a handler parameter of type `socketio.Ack`, which also lets a
handler answer later from another goroutine) and acks for the client's own emits (Phase 3: `EmitWithAck` with a
context, ids per session, pending acks failed when the session ends) were designed in October 2026 and are
held until a consumer needs them. The empty receipt must stay byte-for-byte the default when they arrive.

Calling a future `EmitWithAck` from inside a handler cannot complete while that handler blocks the read loop. That
constraint is why Phase 2's asynchronous ack comes first.

---

## Root namespace — validate, do not ignore

The client supports **only** the default namespace (`""` or `"/"` on the wire).

| `header.Namespace` | Action |
|---|---|
| `""` | OK — implicit root (`42[...]`) |
| `"/"` | OK |
| any other (`/admin`, `/foo`) | **Warn + discard** — do not dispatch handler |

Emit always uses the root namespace. If the server emits on another namespace, the log appears immediately.

Custom namespaces (Phase 4: `client.Of("/ns")` with its own handlers, connection state and `CONNECT_ERROR`,
reconnected with the transport) are deferred: neither the relay nor any consumer uses one.

---

## Cleanup principle (future)

```
integration (connect, emit, receive, reconnect)  ✅
    → soak                                       ✅
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

<details>
<summary>Phase 0 — Session isolation (v1.1.0)</summary>

- One `session` per accepted connection; loops and acks never touch another connection's state
- Synchronous handshake (≤20s); `OnConnect` and the backoff reset only after the server's `CONNECT`
- `CONNECT_ERROR` reported as `*ConnectError` through `OnReconnectAttempt`, retried with backoff
- Encoder writes the namespace separator before an ack id of 0
- In-process integration tests
</details>

<details>
<summary>Phase 1 — Connection control (v1.1.0)</summary>

- `Reconnect()` and `ErrReconnectRequested`
- `WithReconnectBackoff(initial, max, jitter)`; `OnReconnectAttempt` reports the actual wait
- `WithEventErrorHandler` — panics and undecodable payloads keep the session and are still acknowledged
</details>

---

## Pending

- `testdata/` Node server (Socket.IO 4.8, the relay's version) for CI, alongside the in-process tests
- A 24h soak before a consumer moves to a new version in production

**Optional (hygiene):** remove `polling/`, `payload/`, `parser/buffer.go`.

---

## Definition of Done

- [x] No server code
- [x] API: `NewClient`, `Connect`, `Emit`, `On`, `OnConnect`, `OnDisconnect`, `OnReconnectAttempt`, `Reconnect`, `Close`
- [x] WebSocket + EIO=4 + PING→PONG
- [x] Automatic reconnect, configurable backoff with jitter
- [x] HTTP headers on dial
- [x] Server refusals reported, not silent
- [x] Integration tests (in-process server)
- [x] Soak against the relay (100 restarts)
- [x] Clean `go test ./... -race`
- [ ] Integration tests against a Node server in CI

---

## Out of scope (remains)

FSM, Context in the API, rooms, broadcast, server, polling upgrade. Explicit acks, `EmitWithAck` and custom
namespaces are deferred, not out of scope: see above.

---

**Development workflow:** see [DEVELOPMENT.md](./DEVELOPMENT.md).
