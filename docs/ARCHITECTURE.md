# Architecture

The library intentionally follows a minimal design.

```
Application
      │
      ▼
 Client  ── session (one per accepted connection)
      │
      ▼
 parser (Socket.IO packets)
      │
      ▼
 Engine.IO
      │
      ▼
 Gorilla WebSocket
```

## Design principles

- Single exported Client
- WebSocket only
- JSON events
- Automatic reconnect
- Small public API, configured through options
- No unnecessary abstractions

The implementation favors simplicity and maintainability over feature completeness.

## Public API

- `NewClient()` with `WithHeaders`, `WithReconnectBackoff`, `WithEventErrorHandler`
- `Connect()`, `Close()`, `Reconnect()`
- `Emit()`, `On()`
- `OnConnect()`, `OnDisconnect()`, `OnReconnectAttempt()`

See the [README](../README.md) for the behavior of each call.

## Connection loop

`Connect` starts one goroutine, `run`, which owns the whole lifecycle until `Close`:

```
run
 ├─ connectOnce
 │    ├─ dial the WebSocket, read the Engine.IO handshake
 │    └─ handshake: send CONNECT, wait ≤20s for CONNECT or CONNECT_ERROR
 │         ├─ CONNECT        → new session, OnConnect, start its loops
 │         └─ CONNECT_ERROR  → *ConnectError → OnReconnectAttempt → backoff → retry
 ├─ wait for the session to end
 ├─ close the transport, OnDisconnect
 └─ backoff → connectOnce again
```

A connection counts as established only when the server answers `CONNECT`. A refusal is a failed attempt,
so the backoff keeps growing until a connection is accepted.

## Sessions

Each accepted connection gets its own `session`: encoder, decoder, an unbuffered write channel and a `done`
channel closed by the first reason it ends. Both loops receive their session as an argument and never read
connection state from the `Client`:

- **readLoop** decodes packets and runs event handlers inline, one at a time, in arrival order.
- **writeLoop** is the only writer of Socket.IO packets on the transport.

`Emit`, and the ack sent after a handler, hand the packet to a session's write channel and give up when that
session ends. So an ack can only reach the connection its event arrived on, and a loop left over from a
previous connection cannot read from or write to the next one.

The client mutex guards just two pointers: the transport being dialed or in use, and the current session. `Close`
and `Reconnect` act through them.

## Goroutines

During a session: `run`, `readLoop` and `writeLoop`, plus the short-lived goroutines that run `OnConnect` and
`OnDisconnect`. Between sessions, only `run`. After `Close`, none.

Engine.IO PING → PONG is answered inside the Engine.IO reader, on the read loop. A handler that blocks
therefore also delays the PONG.

## Non-goals

This project intentionally does not implement:

- Socket.IO server
- Rooms
- Custom namespaces
- Acknowledgement callbacks with data
- Binary payload API
- HTTP long-polling

Acknowledgement callbacks and namespaces are deferred rather than rejected: see the roadmap in
[CLIENT_EVOLUTION_PLAN.md](./CLIENT_EVOLUTION_PLAN.md).
