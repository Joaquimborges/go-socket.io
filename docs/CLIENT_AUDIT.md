# Complete Technical Audit — Socket.IO Client in Go

> **Note (June 2026):** this document describes the **original** state of the fork (based on googollee/go-socket.io with a minimal client coupled to the server), **before** the refactor described in [CLIENT_EVOLUTION_PLAN.md](./CLIENT_EVOLUTION_PLAN.md). Kept as historical reference and record of decisions.
>
> **Current library state:** client-only, Engine.IO v4, WebSocket, automatic reconnect, `WithHeaders`. See [README.md](../README.md) for usage and API.

**Repository:** `github.com/Joaquimborges/go-socket.io` (fork of feederco/go-socket.io)  
**Audit scope:** **pre-refactor** code, used as a client against **Socket.IO v4** server (Node.js)  
**Audit date:** June 27, 2026  
**Use case:** long-lived connection (days), JSON, dozens of events/minute, Linux, high availability

---

## Post-refactor status (summary)

| Original issue (audit) | Resolution |
|---|---|
| No reconnection | Reconnect with backoff 1s → 30s |
| Engine.IO v3 (`EIO=3`) | Engine.IO v4 (`EIO=4`) |
| PING ignored | PING → PONG in `engineio/client.go` |
| Duplicate `Connect()` leaked goroutines | Idempotent `Connect()` + `ErrAlreadyConnected` |
| Default transport polling | WebSocket-only dial |
| Race in `nextID()` / ACK | ACK removed from API; internal response to `NeedAck` |
| Zero client tests | Basic unit tests; integration pending |
| Callbacks on read goroutine | Kept (documented); handlers must be fast |

**Pending:** integration tests against a real Socket.IO v4 server and long-duration soak.

---

## Executive Summary (original audited state)

This library is, at its core, a **server** Socket.IO implementation with a **minimal client** added later. The client works for basic scenarios against Engine.IO v3-compatible servers, but has **critical gaps** for long-running production use against Socket.IO v4:

| Severity | Issue |
|---|---|
| 🔴 Critical | No reconnection logic |
| 🔴 Critical | Engine.IO v3 hardcoded (`EIO=3`) — incompatible with native Socket.IO v4 |
| 🔴 Critical | Server PING ignored — connection dies after `pingTimeout` |
| 🔴 Critical | Duplicate `Connect()` leaks goroutines and connections |
| 🟠 Important | Default transport is HTTP polling (not websocket) |
| 🟠 Important | Race condition in `nextID()` (ACK IDs) |
| 🟠 Important | Zero automated tests for the client |
| 🟠 Important | Event callbacks run on the read goroutine (no isolation) |

**Verdict:** **Not production-ready** as a long-running Socket.IO v4 client without substantial fixes.

---

## 1. General Architecture

### 1.1 Layered view

```
Application (your Go code)
    │  Emit() / OnEvent() / OnConnect()
    ▼
socketio.Client                    ← client.go
    │  clientRead / clientWrite / clientError (3 goroutines)
    ▼
socketio.conn                      ← connection.go
    │  writeChan / errorChan / quitChan
    │  parser.Encoder / parser.Decoder
    ▼
engineio.client                    ← engineio/client.go
    │  serve() — heartbeat goroutine (PING)
    │  NextReader() / NextWriter()
    ▼
transport.Conn                     ← polling or websocket
    │  polling: HTTP long-poll (GET/POST) + payload.Payload
    │  websocket: gorilla/websocket + wrapper with mutex
    ▼
TCP / TLS
```

### 1.2 Connection flow

```
1. NewClient(addr, opts)
   └─ Parse URL, extract namespace from path, build URL /socket.io/{namespace}

2. Client.Connect()
   ├─ engineio.Dialer.Dial(url)          [EIO=3 hardcoded]
   │   └─ Try transports (default: polling only)
   │       ├─ HTTP GET → receive OPEN packet (sid, pingInterval, pingTimeout)
   │       └─ Start goroutines: getOpen, serveGet, servePost (polling)
   ├─ newConn(engineConn, handlers)
   ├─ connectClient() → send Socket.IO CONNECT packet (type 40)
   └─ go clientError / clientWrite / clientRead
```

**File:** `client.go:66-91`

### 1.3 Engine.IO handshake

The dialer adds `EIO=3` to the query string:

```go
// engineio/dialer.go:28-30
query := u.Query()
query.Set("EIO", "3")
```

The server responds with an OPEN packet containing JSON:

```json
{"sid":"...","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000}
```

Parsed in `transport.ReadConnParameters()`.

### 1.4 Socket.IO handshake

After Engine.IO is connected, `connectClient()` sends a CONNECT packet on the root namespace:

```go
// client.go:264-284
header := parser.Header{Type: parser.Connect}
return c.encoder.Encode(header)  // wire: "40"
```

The server responds with `40` (or `40{...}` with sid/auth). Handler: `clientConnectPacketHandler()`.

### 1.5 Reading

```
clientRead() [1 goroutine]
  └─ infinite loop:
       decoder.DecodeHeader(&header, &event)  → engineio.NextReader() → transport
       switch header.Type:
         Connect    → clientConnectPacketHandler
         Disconnect → clientDisconnectPacketHandler
         Event      → eventPacketHandler → OnEvent callback
         Ack        → ackPacketHandler
```

### 1.6 Writing

```
Emit(event, args...)
  └─ namespaceConn.Emit()
       └─ conn.write(header, args)  → writeChan (unbuffered)
            └─ clientWrite() [1 goroutine]
                 └─ encoder.Encode() → engineio.NextWriter() → transport
```

**Correct pattern:** a single write goroutine serializes all writes. Multiple goroutines can call `Emit()` safely with respect to the websocket (as long as they go through `writeChan`).

### 1.7 Heartbeat

```
engineio/client.serve() [1 goroutine, created on Dial]
  └─ loop:
       time.After(pingInterval)
       NextWriter(PING) → send ping to server
       SetWriteDeadline(...)
```

Server PONG response → `NextReader()` updates `SetReadDeadline`.

### 1.8 Reconnection

**Does not exist.** No file implements reconnect, retry, or backoff. Searching for `reconnect`, `Reconnect`, `retry` in the code returns only internal `retryError` from payload (transport upgrade), not application reconnection.

---

## 2. Concurrency

### 2.1 Direct answers

| Question | Answer |
|---|---|
| Is there only one goroutine reading? | **Yes** — `clientRead()` |
| Is there only one goroutine writing? | **Yes** — `clientWrite()` |
| Can multiple goroutines call `Emit()` simultaneously? | **Yes** — via `writeChan` |
| Is there a mutex protecting the connection? | **Partial** — websocket has `writeLocker`/`readLocker`; polling uses `payload.Payload` with atomic CAS |
| Is there a write channel? | **Yes** — `writeChan chan parser.Payload` (unbuffered) |
| Risk of "concurrent write to websocket"? | **Low** — serialized by `clientWrite` + mutex on wrapper |
| Risk of data race? | **Yes** — see issues below |

### 2.2 Write serialization (correct)

```go
// connection.go:115-131
func (c *conn) write(header parser.Header, args ...reflect.Value) {
    select {
    case c.writeChan <- pkg:    // unbuffered — blocks until clientWrite consumes
    case <-c.quitChan:
        return
    }
}
```

```go
// engineio/transport/websocket/wrapper.go:93-113
func (w wrapper) NextWriter(...) {
    w.writeLocker.Lock()        // mutex — prevents concurrent WriteMessage
    ...
}
```

### 2.3 Data race: unprotected `nextID()`

```go
// connection.go:109-113
func (c *conn) nextID() uint64 {
    c.id++    // ← NO mutex, NO atomic
    return c.id
}
```

Called from `namespaceConn.Emit()` when there is an ACK callback:

```go
// namespace_conn.go:72-78
if lastV.Kind() == reflect.Func {
    header.ID = nc.conn.nextID()   // race if multiple goroutines emit with ACK
    nc.ack.Store(header.ID, f)
}
```

**Real scenario:** two goroutines call `Emit("event", data, ackFn)` simultaneously → duplicate IDs → ACKs swapped or lost.

### 2.4 Data race: namespace `context`

```go
// namespace_conn.go:47-52
func (nc *namespaceConn) SetContext(ctx interface{}) {
    nc.context = ctx    // no lock
}
```

Documentation says "no need to lock" assuming single-threaded access, but callbacks run on the read goroutine while other goroutines may call `SetContext`/`Emit` concurrently.

### 2.5 Concurrent callbacks with reading

Comment in `namespace_conn.go:14-16`:

> "The handlers are called in one goroutine, so no need to lock context"

**Partial truth:** event handlers run on the `clientRead` goroutine, but `Emit()` can be called from any goroutine. If a handler calls `Emit()` and blocks waiting for ACK, and the ACK arrives on the same read goroutine → **classic deadlock**.

**Real scenario:**

```go
client.OnEvent("request", func(c socketio.Conn) {
    c.Emit("response", data, func(result string) { ... })  // blocks writeChan
    // ACK must be read by clientRead — same goroutine → DEADLOCK
})
```

### 2.6 Unbuffered `errorChan`

```go
// connection.go:134-139
func (c *conn) onError(namespace string, err error) {
    select {
    case c.errorChan <- newErrorMessage(namespace, err):
    case <-c.quitChan:
    }
}
```

If `clientError` is running a slow `OnError`, subsequent calls to `onError` block the calling goroutine (may be `clientWrite` or `clientRead`).

---

## 3. Goroutines

### 3.1 Complete map (client connected via polling)

| Goroutine | Created by | Stopped by | Leak risk |
|---|---|---|---|
| `clientRead` | `Client.Connect()` | Decode error or `quitChan` | Medium |
| `clientWrite` | `Client.Connect()` | `quitChan` | Low |
| `clientError` | `Client.Connect()` | `quitChan` | Low |
| `engineio.client.serve()` | `Dialer.Dial()` | `c.close` channel | Low |
| `polling.getOpen()` | `clientConn.Open()` | Return after FeedIn or error | Medium |
| `polling.serveGet()` | `clientConn.Open()` | HTTP error or Close | Medium |
| `polling.servePost()` | `clientConn.Open()` | HTTP error or Close | Medium |
| `rcWrapper/wcWrapper nagger` | Each NextReader/NextWriter (ws) | Wrapper Close | Low |

### 3.2 Issue: duplicate `Connect()`

```go
// client.go:66-91 — does NOT check if already connected
func (c *Client) Connect() error {
    enginioCon, err := dialer.Dial(c.url, nil)
    ...
    c.conn = newConn(enginioCon, c.handlers)  // replaces previous conn
    go c.clientError()
    go c.clientWrite()
    go c.clientRead()
}
```

**Real scenario:** manual reconnection calling `Connect()` again → 3 old goroutines + old polling goroutines keep running referencing the old connection → **goroutine leak, TCP connection leak, duplicate messages**.

### 3.3 Issue: triple `Close()` in defers

Three goroutines (`clientRead`, `clientWrite`, `clientError`) have:

```go
defer func() {
    if err := c.Close(); err != nil { ... }
}()
```

`closeOnce` protects against double-close, but whichever goroutine exits first triggers global `Close()`, disconnecting everything. Correct behavior but fragile — if `clientRead` returns due to decode error, `clientWrite` and `clientError` also close via defer cascade.

### 3.4 Stuck goroutines

- **`servePost`/`serveGet`:** if `Payload.FeedIn` returns an error without calling `Close()`, the goroutine returns but others may stay blocked on `NextReader` waiting for data.
- **`clientWrite` blocked on `writeChan`:** if nobody consumes (bug), goroutine stuck forever.
- **`time.After` in loops:** `engineio/client.serve()` and `payload.Payload` use `time.After` in loops — timers accumulate until they fire (see section 9).

---

## 4. Channels

### 4.1 Inventory

| Channel | Buffer | Created | Closed | Producer | Consumer |
|---|---|---|---|---|---|
| `writeChan` | 0 | `newConn()` | **Never** | `conn.write()` | `clientWrite()` |
| `errorChan` | 0 | `newConn()` | **Never** | `conn.onError()` | `clientError()` |
| `quitChan` | 0 | `newConn()` | `conn.Close()` | — | 3 client goroutines |
| `engineio.client.close` | 0 | `Dialer.Dial()` | `client.Close()` | — | `serve()` |
| `payload.readerChan` | 0 | `payload.New()` | `Payload.Close()` | `FeedIn()` | `getReader()` |
| `payload.writerChan` | 0 | `payload.New()` | `Payload.Close()` | `FlushOut()` | `getWriter()` |
| `payload.readError` | 0 | `payload.New()` | **Never** | `putReader()` | `FeedIn()` |
| `payload.writeError` | 0 | `payload.New()` | **Never** | `putWriter()` | `FlushOut()` |

### 4.2 Potential deadlock: Emit + ACK on same goroutine

Described in section 2.5. The unbuffered `writeChan` blocks `Emit()` until `clientWrite` processes, but if the caller is `clientRead` processing an event, nobody reads the response ACK.

### 4.3 Blocking on `Emit()` during shutdown

```go
select {
case c.writeChan <- pkg:
case <-c.quitChan:
    return
}
```

If `clientWrite` already exited but `quitChan` has not been read by another goroutine emitting → **indefinite block** on `writeChan` (no default/timeout).

### 4.4 Channels never closed

`writeChan` and `errorChan` are not closed in `Close()`. They rely on `quitChan` to unblock via `select`. Functional but prevents clean range/drain.

---

## 5. Reconnection

### 5.1 Current state

**Reconnection is not implemented.** Complete analysis:

| Criterion | Status |
|---|---|
| Automatic reconnect | ❌ Does not exist |
| Recreates state correctly | ❌ N/A |
| Cleans up old resources | ❌ Duplicate `Connect()` does not clean up |
| Can open multiple connections | ✅ Yes — bug |
| Leaves old goroutines alive | ✅ Yes — bug |
| Callbacks registered again | N/A — handlers persist in `Client.handlers`, but namespace conns are recreated |

### 5.2 What happens when the connection drops

1. `clientRead` receives error in `DecodeHeader` → log + `onError` + **return**
2. Defer calls `Close()` → `quitChan` closed, disconnect handlers called
3. **`Client` becomes unusable** — no automatic reconnect nor `Reconnect()` method
4. Application must create a new `Client` manually

### 5.3 Implication for long duration

For connections lasting days, network drops, Node.js server restarts, or load balancer timeouts **require reconnection logic in the application**. The library offers no abstraction for this.

---

## 6. Heartbeat

### 6.1 Implemented mechanism

**Client initiates PING** (`engineio/client.go:105-135`):

```go
func (c *client) serve() {
    for {
        select {
        case <-c.close:
            return
        case <-time.After(c.params.PingInterval):
        }
        w, err := c.conn.NextWriter(frame.String, packet.PING)
        ...
        w.Close()
        c.conn.SetWriteDeadline(time.Now().Add(c.params.PingInterval + c.params.PingTimeout))
    }
}
```

**Client processes PONG** (`engineio/client.go:63-66`):

```go
case packet.PONG:
    c.conn.SetReadDeadline(time.Now().Add(c.params.PingInterval + c.params.PingTimeout))
```

### 6.2 CRITICAL BUG: server PING ignored

The server (Session) responds correctly to PING:

```go
// engineio/session/session.go:95-115 — SERVER (correct reference)
case packet.PING:
    w, err := s.nextWriter(ft, packet.PONG)
    io.Copy(w, r)  // echo
    w.Close()
    r.Close()
```

But the **client** does NOT implement this:

```go
// engineio/client.go:62-82 — CLIENT
switch pt {
case packet.PONG:
    SetReadDeadline(...)
case packet.CLOSE:
    Close(); return io.EOF
case packet.MESSAGE:
    return session.FrameType(ft), r, nil
}
// PING falls through to default: close reader and continue loop WITHOUT sending PONG
if err = r.Close(); err != nil { ... }
```

**Socket.IO v4 / Engine.IO v4:** the Node.js server sends PING every `pingInterval` (default 25s). If the client does not respond with PONG within `pingTimeout` (default 20s), the server **disconnects**.

**Real scenario:** websocket connection established → server sends PING at 25s → Go client discards → at 45s server closes connection → `clientRead` receives EOF → dead connection. Exactly the opposite of what is needed for a "long-lived connection".

### 6.3 Incorrect dead connection detection

- **Initial read deadline never set** — only after receiving first PONG (response to client PING)
- If the server only sends PING (does not respond to client PING), the deadline is never updated via PONG
- Client may think connection is alive while server already disconnected

### 6.4 `time.After` in heartbeat loop

```go
case <-time.After(c.params.PingInterval):  // new timer every iteration
```

On connections lasting weeks, pending timers accumulate → GC pressure (see section 9).

---

## 7. gorilla/websocket usage

### 7.1 Positive points

- **Read/write mutex** via `wrapper` — prevents concurrent Read/Write
- **ReadCloser/WriteCloser wrappers** ensure unlock after Close()
- **Nag timer** (30s) alerts if Close() was not called on wrappers
- **SetWriteDeadline** protected by mutex

### 7.2 Issues

| Issue | Detail |
|---|---|
| Client uses polling by default | `client.go:68` — websocket never used unless manually configured on Dialer |
| No initial SetReadDeadline | Websocket connection without read timeout until first PONG |
| Upgrade not implemented on client | Client stays on polling even if server offers websocket |
| Default buffer sizes | `ReadBufferSize`/`WriteBufferSize` = 0 (Go default) — OK for small JSON |

### 7.3 Concurrent write — mitigated

All writes go through `clientWrite` → `encoder.Encode` → `NextWriter` → `writeLocker.Lock()`. **No risk** of "concurrent write to websocket connection" as long as the flow does not bypass the channel.

---

## 8. Safety

### 8.1 Possible panics

```go
// client.go:95-96
func (c *Client) Close() error {
    return c.conn.Close()  // PANIC if conn == nil (before Connect())
}
```

```go
// handler.go:37, 42, 64 — panic on handler registration
panic("event handler must be a func.")
panic("handler function should be like func(socketio.Conn, ...)")
```

```go
// namespace_handler.go:109
msg = args[0].Interface().(string)  // panic if wrong type
```

### 8.2 Nil pointer

- `Client.Close()` before `Connect()` → nil pointer dereference
- `Client.Emit()` before `Connect()` → log "not initialized", returns silently (no error)

### 8.3 Concurrent access

- `namespaceConn.context` — no protection
- `conn.id` — no protection (ACK IDs)
- `namespaceHandler.events` — protected by `RWMutex` ✅
- `namespaces` map — protected by `RWMutex` ✅
- `ack sync.Map` — thread-safe ✅

### 8.4 Concurrent callbacks

Event handlers run on the read goroutine. Multiple events are processed **sequentially** (not parallel), but `Emit()` from other goroutines is concurrent with handlers.

---

## 9. Memory Leak

### 9.1 Timers never stopped

**`engineio/client.serve()`:**

```go
case <-time.After(c.params.PingInterval):  // classic time.After leak in loop
```

**`engineio/payload/payload.go` — `readTimeout()`/`writeTimeout()`:**

```go
return time.After(wait), true  // called in select loops
```

On connections active for weeks with polling, hundreds of pending timers accumulate memory until GC.

**Fix:** use `time.NewTicker` or `time.NewTimer` with `Stop()`/`Reset()`.

### 9.2 Orphan ACK callbacks

```go
// namespace_conn.go:78
nc.ack.Store(header.ID, f)
// Removed only in ackPacketHandler via defer nc.ack.Delete(header.ID)
```

If ACK never arrives (server does not respond, connection drops), entry remains in `sync.Map` indefinitely.

### 9.3 Orphan goroutines

Duplicate `Connect()` (section 3.2) — main source of goroutine accumulation.

### 9.4 Accumulating objects

- `errorChan`/`writeChan` without drain on Close
- Nag timer goroutines on websocket (2 per Read/Write operation) — terminate after 30s or Close

---

## 10. Public API

### 10.1 Current API

```go
client, err := socketio.NewClient("http://host:port/namespace", opts)
client.OnEvent("event", func(c socketio.Conn, msg string) { ... })
client.OnConnect(func(c socketio.Conn) error { ... })
client.OnDisconnect(func(c socketio.Conn, reason string) { ... })
client.OnError(func(c socketio.Conn, err error) { ... })
err := client.Connect()
client.Emit("event", data...)
client.Close()
```

### 10.2 API issues

| Issue | Impact |
|---|---|
| No `context.Context` | Cannot cancel Connect/Emit with timeout |
| Blocking `Connect()` without timeout | Can block indefinitely on DNS/TCP |
| Fire-and-forget `Emit()` | No error return, no delivery confirmation |
| `Close()` can panic | If called before Connect |
| No connection state | No `IsConnected()`, `ConnectionState()` |
| No reconnection | Application must reimplement everything |
| `opts *engineio.Options` ignored | `Client.opts` never used in Connect |
| Transport not configurable | Hardcoded polling.Default |
| No exposed TLS config | Requires modifying source code |

### 10.3 Suggested API

```go
type Client interface {
    Connect(ctx context.Context) error
    Close() error
    Emit(ctx context.Context, event string, args ...any) error
    On(event string, handler EventHandler)
    ConnectionState() ConnectionState
    Done() <-chan struct{}  // signals permanent disconnection
}

type Options struct {
    URL              string
    Namespace        string
    Transport        []transport.Transport  // default: websocket
    Reconnect        ReconnectConfig
    TLS              *tls.Config
    Headers          http.Header
    Auth             map[string]any         // Socket.IO v4 auth
    PingHandler      func()
    Logger           *slog.Logger
}
```

---

## 11. Socket.IO v4 compatibility

### 11.1 Engine.IO

| Aspect | Socket.IO v4 expects | This library | Compatible? |
|---|---|---|---|
| Protocol version | EIO=4 | EIO=3 hardcoded | ❌ |
| OPEN packet | JSON with maxPayload | Ignores maxPayload | ⚠️ |
| PING/PONG | Server ping → client pong | Client ping only, ignores server ping | ❌ |
| WebSocket transport | Preferred | Polling default, no upgrade | ⚠️ |
| Binary frames | Supported | Supported (not required) | ✅ |

### 11.2 Socket.IO protocol

| Aspect | v4 | This library | Compatible? |
|---|---|---|---|
| CONNECT (40) | Optional auth JSON | Sends 40 without auth | ⚠️ |
| EVENT (42) | JSON array `[event, ...args]` | Implemented | ✅ |
| ACK (43) | With numeric ID | Implemented | ✅ |
| DISCONNECT (41) | Implemented | Implemented | ✅ |
| Binary events (45/46) | Supported | Implemented | ✅ (not required) |
| Namespace | `/ns,` prefix | Implemented | ✅ |
| Protocol version header | v5 (implicit) | Not sent | ⚠️ |

### 11.3 Expected practical test against Socket.IO v4

```
1. Dial with EIO=3 → v4 server may reject (400 Bad Request) or accept in legacy mode
2. If accepted → CONNECT works for default namespace
3. EVENT/ACK works for simple JSON
4. Heartbeat fails in ~25-45s → guaranteed disconnection
```

**README confirms:** "supports 1.4 version of the Socket.IO client" — not v4.

### 11.4 Critical EIO v3 vs v4 differences

- v4 adds `maxPayload` in OPEN
- v4 changed format of some packets
- Modern Socket.IO v4 servers may disable EIO v3 completely
- v4 requires response to server PING (server-initiated heartbeat)

---

## 12. Robustness (weeks of continuous execution)

### 12.1 Failure scenarios

| Scenario | Behavior | Severity |
|---|---|---|
| Network drop | Connection dies, no reconnect | 🔴 |
| Node server restart | Connection dies, no reconnect | 🔴 |
| Server PING timeout | Disconnection in ~45s | 🔴 |
| Load balancer idle timeout | Depends on transport; polling may keep alive | 🟡 |
| Accidental double `Connect()` | Goroutine/connection leak | 🔴 |
| ACK timeout | Entry remains in sync.Map | 🟡 |
| `time.After` accumulation | Slow memory growth | 🟡 |
| Slow handler blocks reading | Subsequent events delayed | 🟡 |
| Emit during disconnect | May block indefinitely | 🟠 |
| HTTP Client timeout (polling, 1min) | Long-poll > 1min fails | 🟠 |

### 12.2 HTTP Client timeout on polling

```go
// engineio/transport/polling/transport.go:19-23
var Default = &Transport{
    Client: &http.Client{
        Timeout: time.Minute,  // 60 seconds max per request
    },
}
```

If `pingInterval + pingTimeout > 60s` (configurable on server), long-poll GET expires → connection drops.

---

## 13. Improvements by priority

### 🔴 Critical (fix before production)

1. **Implement response to server PING** — add `case packet.PING` in `engineio/client.go:NextReader()` mirroring `session/session.go`
2. **Change EIO=3 to EIO=4** — `engineio/dialer.go:29` + validate OPEN parsing
3. **Implement reconnection** — exponential backoff, re-handshake, re-CONNECT namespace
4. **Protect `Connect()` against double call** — return error or close previous connection
5. **Protect `Client.Close()` against nil** — `if c.conn != nil`
6. **Use websocket as default transport** — not polling for long connections
7. **Fix race in `nextID()`** — `atomic.AddUint64`

### 🟠 Important (recommended)

8. Add `context.Context` to Connect/Emit/Close
9. Buffer `writeChan` (e.g. 256) to avoid blocking producers
10. Run event handlers in a separate goroutine (worker pool) to avoid ACK deadlock
11. Replace `time.After` with `time.NewTicker` in heartbeat loops
12. Add `IsConnected()` and `Disconnected()` channel
13. Expose transport and TLS configuration via `Client.opts`
14. Timeout on `Emit()` with context
15. Clean orphan ACKs with timer
16. Integration tests client ↔ real Socket.IO v4 server
17. Graceful shutdown: drain `writeChan` before closing

### 🟢 Optional (refactoring)

18. `Client` interface for testability
19. Structured logging (slog) instead of custom logger
20. Metrics (prometheus): connected, reconnects, events/s, errors
21. Auth support in CONNECT packet (Socket.IO v4)
22. Polling → websocket upgrade on client
23. Client-specific documentation
24. Remove rooms/broadcast dependency on client (dead code for use case)

---

## 14. Detailed evidence

### E1 — Server PING ignored (CRITICAL)

**File:** `engineio/client.go`  
**Function:** `(*client).NextReader()`  
**Lines:** 55-82

**Flow:**
1. Server sends Engine.IO PING packet
2. `NextReader()` receives via transport
3. Switch has no `packet.PING` case
4. Falls through: `r.Close()` and loop continues
5. Server never receives PONG
6. After `pingTimeout`, server closes connection

**Why it exists:** Client code was written assuming unidirectional heartbeat (client initiates PING via `serve()`), partially copying the server pattern (`session.go`) but omitting the incoming PING handler.

**Proposed fix:**

```go
case packet.PING:
    w, err := c.conn.NextWriter(ft, packet.PONG)
    if err != nil {
        return 0, nil, err
    }
    if _, err = io.Copy(w, r); err != nil {
        w.Close()
        r.Close()
        return 0, nil, err
    }
    w.Close()
    r.Close()
    if err = c.conn.SetReadDeadline(time.Now().Add(c.params.PingInterval + c.params.PingTimeout)); err != nil {
        return 0, nil, err
    }
```

---

### E2 — EIO=3 hardcoded (CRITICAL)

**File:** `engineio/dialer.go`  
**Function:** `(*Dialer).Dial()`  
**Lines:** 28-30

```go
query.Set("EIO", "3")
```

**Flow:** Every connection sends `?EIO=3&transport=polling`. Socket.IO v4 server expects `EIO=4`.

**Real scenario:** Deploy against strict Socket.IO v4 server → HTTP 400 "Unsupported protocol version" → `Connect()` returns error → application never connects.

**Fix:** `query.Set("EIO", "4")` + tests against real v4 server. Consider making it configurable.

---

### E3 — Duplicate Connect() leaks goroutines (CRITICAL)

**File:** `client.go`  
**Function:** `(*Client).Connect()`  
**Lines:** 66-91

**Flow:**
1. First `Connect()` → creates conn1 + 3 goroutines + polling goroutines
2. Second `Connect()` → creates conn2, overwrites `c.conn`
3. conn1 goroutines keep running, referencing conn1 (orphan)
4. conn1 never explicitly closed

**Real scenario:** Application reconnection logic calls `Connect()` again → after N reconnections, N×3 socketio goroutines + N×3 polling goroutines accumulated → OOM or file descriptor exhaustion.

**Fix:**

```go
func (c *Client) Connect() error {
    if c.conn != nil {
        if err := c.Close(); err != nil {
            return fmt.Errorf("close previous connection: %w", err)
        }
    }
    // ... rest of dial
}
```

---

### E4 — Client.Close() nil panic (CRITICAL)

**File:** `client.go`  
**Function:** `(*Client).Close()`  
**Lines:** 95-97

```go
func (c *Client) Close() error {
    return c.conn.Close()
}
```

**Real scenario:** `defer client.Close()` before `Connect()` on error path → panic → process crash.

**Fix:**

```go
func (c *Client) Close() error {
    if c.conn == nil {
        return nil
    }
    return c.conn.Close()
}
```

---

### E5 — nextID() race condition (IMPORTANT)

**File:** `connection.go`  
**Function:** `(*conn).nextID()`  
**Lines:** 109-113

**Real scenario:** 10 goroutines emit events with ACK callback simultaneously → IDs 5 and 5 assigned → event A ACK delivers response to event B callback.

**Fix:**

```go
func (c *conn) nextID() uint64 {
    return atomic.AddUint64(&c.id, 1)
}
```

---

### E6 — Emit+ACK deadlock in handler (IMPORTANT)

**File:** `namespace_conn.go` + `client.go`  
**Functions:** `Emit()` → `write()` → `clientRead()` → handler

**Real scenario:**

```go
client.OnEvent("getData", func(c socketio.Conn) {
    done := make(chan struct{})
    c.Emit("fetch", id, func(data Data) {
        process(data)
        close(done)
    })
    <-done  // waits for ACK
})
// clientRead is blocked on <-done
// clientWrite needs to send "fetch" but ACK response must be read by clientRead
// DEADLOCK
```

**Fix:** Run handlers in a separate goroutine:

```go
go func() {
    err := handler.dispatchEvent(conn, event, args...)
    ...
}()
```

Or explicitly document that ACK callbacks cannot be used inside event handlers.

---

### E7 — Polling default transport (IMPORTANT)

**File:** `client.go`  
**Lines:** 67-69

```go
dialer := engineio.Dialer{
    Transports: []transport.Transport{polling.Default},
}
```

**Real scenario:** Long connection via HTTP polling → HTTP header overhead on every event (POST) and long-poll (GET) → higher latency, more CPU, more vulnerable to proxy timeouts vs websocket single connection.

**Fix:**

```go
dialer := engineio.Dialer{
    Transports: []transport.Transport{
        websocket.Default,
        polling.Default,
    },
}
```

And implement upgrade on the client.

---

### E8 — opts ignored (IMPORTANT)

**File:** `client.go`  
**Field:** `opts *engineio.Options` (line 26)

`Connect()` creates local Dialer without using `c.opts`. PingInterval, PingTimeout, Transports, TLS settings are ignored.

**Fix:** Use `c.opts` to configure Dialer and transports.

---

### E9 — Zero client tests

**Evidence:** `grep -r "NewClient" *_test.go` → zero results.

All test coverage (passing with `-race`) is for the **server** and internal engineio. The client has no unit or integration tests.

---

### E10 — time.After leak (IMPORTANT)

**File:** `engineio/client.go:116`, `engineio/payload/payload.go:274,287`

**Real scenario:** Connection active for 30 days, pingInterval=25s → ~103,000 timers created and pending until fire → GC pressure, growing RSS.

**Fix:**

```go
ticker := time.NewTicker(c.params.PingInterval)
defer ticker.Stop()
for {
    select {
    case <-c.close:
        return
    case <-ticker.C:
        // send ping
    }
}
```

---

## 15. Final classification

| Category | Score (0-10) | Justification |
|---|---|---|
| **Architecture** | 5/10 | Read/write/error goroutine separation is correct, but client is an afterthought on server codebase |
| **Concurrency** | 4/10 | Write serialization OK, but races in nextID, ACK deadlock, callbacks on read goroutine |
| **Reconnection** | 0/10 | Nonexistent; duplicate Connect() makes it worse |
| **Robustness** | 2/10 | Incomplete heartbeat kills connection; no recovery; timer and goroutine leaks |
| **Readability** | 6/10 | Idiomatic Go, mirrors server patterns, but client underdocumented |
| **Safety** | 5/10 | No panics on common paths (except Close nil), reflection handlers with recover |
| **Production readiness** | **2/10** | Not recommended for long-running Socket.IO v4 without critical fixes |

### Overall score: **3/10**

---

## Conclusion (original audit)

### Production ready? **NO** *(at audit date)*

After the refactor (PRs 1–4 + headers), the library addresses the critical items below. Final validation depends on integration tests and soak.

### What needed to be fixed (historical checklist):

1. ✅ Respond to server PING (without this, connection dies in seconds/minutes)
2. ✅ Migrate to Engine.IO v4 (`EIO=4`)
3. ✅ Implement reconnection with backoff and resource cleanup
4. ✅ Use websocket as primary transport
5. ✅ Protect against duplicate Connect() and nil Close()
6. ✅ Fix ACK ID race condition *(ACK removed from public API)*
7. ⏳ Add integration tests against real Socket.IO v4
8. ✅ Resolve Emit+ACK deadlock in event handlers *(async OnConnect + Encode fix)*

### Alternative path *(original audit context)*

If library fixes are not viable in the short term, consider:

- **`github.com/maldikhan/go.socket.io`** or **`github.com/feederco/go-socket.io`** (check if fork has fixes)
- **Socket.IO client via Node.js sidecar** with gRPC/HTTP communication
- **Custom minimal client** using gorilla/websocket + Socket.IO v4 protocol (~500 lines for simple use case)

For the described use case (JSON, long duration, stability), a dedicated websocket client with correct heartbeat and reconnect would be significantly more reliable than this library in its current state.

---

## Appendix A — Relevant file map

| File | Role |
|---|---|
| `client.go` | Socket.IO client public API |
| `connection.go` | Internal Conn, channels, write/read |
| `connection_handlers.go` | Packet handlers (connect/event/ack/disconnect) |
| `namespace_conn.go` | Emit, ACK, namespace |
| `namespace_handler.go` | Callback registration and dispatch |
| `parser/encoder.go` | Socket.IO protocol encoding |
| `parser/decoder.go` | Socket.IO protocol decoding |
| `engineio/dialer.go` | Engine.IO dial (EIO version) |
| `engineio/client.go` | Engine.IO client (heartbeat, NextReader/Writer) |
| `engineio/session/session.go` | Correct heartbeat reference (server) |
| `engineio/transport/polling/connect.go` | HTTP long-polling client |
| `engineio/transport/websocket/wrapper.go` | gorilla/websocket mutex |
| `engineio/transport/websocket/transport.go` | Websocket dial |
| `engineio/payload/payload.go` | Polling payload encode/decode |

## Appendix B — Tests run

```
go test ./... -count=1     → OK (no client tests)
go test ./... -race        → OK (race detector clean on existing tests)
```

**Note:** Clean race detector on existing tests **does not cover** client code, which has no tests.
