# go-socket.io

A lightweight and production-oriented Socket.IO v4 client for Go.

This repository is a focused fork of the original `feederco/go-socket.io`, redesigned to provide a simple and reliable Socket.IO client for applications that keep one connection open for days.

The project intentionally focuses on:

- Socket.IO v4 (protocol v5) over Engine.IO v4
- WebSocket transport
- Automatic reconnect with configurable backoff
- Long-lived connections
- JSON events

It does **not** aim to be a complete Socket.IO implementation.

---

## Installation

```bash
go get github.com/Joaquimborges/go-socket.io
```

---

## Quick Start

```go
package main

import (
	"log"
	"net/http"

	socketio "github.com/Joaquimborges/go-socket.io"
)

type Message struct {
	Username string `json:"username"`
	Message  string `json:"message"`
}

func main() {
	client, err := socketio.NewClient("http://localhost:8083",
		socketio.WithHeaders(http.Header{"X-Client-Key": {"secret"}}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Register every handler and callback before Connect.
	client.On("show_message", func(msg Message) {
		log.Printf("%s: %s", msg.Username, msg.Message)
	})

	client.OnConnect(func() {
		log.Println("connected")

		if err := client.Emit("show_message", Message{Username: "Pedro", Message: "Hello!"}); err != nil {
			log.Println("emit failed:", err)
		}
	})

	client.OnDisconnect(func(err error) {
		log.Println("disconnected:", err)
	})

	if err := client.Connect(); err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	select {}
}
```

The address is the server's base URL (`http`, `https`, `ws` or `wss`). The client always connects to the
`/socket.io/` path on the root namespace, whatever path the address carries.

---

## API

### Client

| Call | Description |
|---|---|
| `NewClient(addr, opts...) (*Client, error)` | Creates the client. Returns `ErrEmptyAddr`, a URL parse error or `ErrInvalidOption` |
| `Connect() error` | Starts the connection loop in the background and returns at once. A second call returns `ErrAlreadyConnected` |
| `Close() error` | Stops reconnecting and closes the transport. Final: the client cannot connect again |
| `Emit(event, args...) error` | Sends an event with JSON-encoded arguments. Returns `ErrNotConnected` when no session is open |
| `On(event, handler)` | Registers the handler for an event. Its parameters are decoded from the event's JSON arguments, in order |
| `Reconnect() error` | Ends the current session and connects again after the initial backoff. Returns `ErrNotConnected` when no session is open |

### Callbacks

| Call | When it runs |
|---|---|
| `OnConnect(func())` | Each time the server accepts the connection, including after every reconnect. Runs on its own goroutine |
| `OnDisconnect(func(err error))` | Each time an accepted session ends. Runs on its own goroutine |
| `OnReconnectAttempt(func(attempt int, backoff time.Duration, err error))` | Each time a connection attempt fails, with the wait before the next one. Runs on the reconnect loop, so keep it short |

Handlers and callbacks are read without locking by the client's goroutines: register them all before `Connect`.

### Options

| Option | Default | Description |
|---|---|---|
| `WithHeaders(http.Header)` | none | HTTP headers sent on every connection attempt, including reconnects |
| `WithReconnectBackoff(initial, max, jitter)` | `1s`, `30s`, `0` | The wait between attempts starts at `initial`, doubles up to `max` and returns to `initial` after an accepted connection. Each wait is drawn from `(backoff×(1−jitter), backoff]` |
| `WithEventErrorHandler(func(event string, err error))` | none | Keeps the session when a handler panics or a payload does not decode. See [Acknowledgements](#acknowledgements) |

### Errors

| Error | Meaning |
|---|---|
| `ErrEmptyAddr` | `NewClient` received an empty address |
| `ErrInvalidOption` | An option was given an invalid value |
| `ErrAlreadyConnected` | `Connect` was called more than once |
| `ErrNotConnected` | `Emit` or `Reconnect` without an open session |
| `ErrReconnectRequested` | What `OnDisconnect` receives after `Reconnect` |
| `*ConnectError` | The server refused the connection. `Message` and `Data` carry what the server sent |

---

## Connection lifecycle

1. **Dial.** The client opens a WebSocket with `EIO=4` and reads the Engine.IO handshake.
2. **Handshake.** It sends `CONNECT` on the root namespace and waits up to 20 seconds for the server's answer.
   Only `CONNECT` from the server counts as connected: that is when `OnConnect` runs and the backoff resets.
3. **Refusal.** A `CONNECT_ERROR` (for example a server middleware rejecting the credentials) is a failed
   attempt: `OnReconnectAttempt` receives a `*ConnectError` and the client retries with backoff.
4. **Session.** One read loop and one write loop run per session. Engine.IO pings are answered automatically,
   and a server silent for longer than `pingInterval + pingTimeout` ends the session.
5. **End.** A transport error, a server `DISCONNECT`, a write error or `Reconnect` ends the session.
   `OnDisconnect` runs and the client reconnects after the backoff. Unlike the JavaScript client, a server
   `DISCONNECT` is reconnected too. Only `Close` stops the loop.

`Emit` never queues for a later session: while there is none it returns `ErrNotConnected`, and the caller decides
whether to retry.

---

## Handlers run on the read loop

Event handlers run synchronously on the session's read loop, one at a time, in the order the events arrive.
While a handler runs, nothing else is read on that connection: no other event and no Engine.IO ping.

Keep handlers short. For slow work such as hardware I/O, hand the event to a worker of your own and return. A
single worker goroutine fed by a channel keeps the arrival order. A goroutine per event does not:

```go
type command struct {
	Button  string `json:"button"`
	Command int    `json:"command"`
}

commands := make(chan command, 64)

go func() {
	for cmd := range commands {
		apply(cmd) // slow, ordered, off the read loop
	}
}()

client.On("POST::player::command", func(cmd command) {
	commands <- cmd // returns at once, so the ack goes out at once
})
```

Calling `Emit` from inside a handler is safe. It waits only for the write loop.

---

## Acknowledgements

When the server emits with an acknowledgement (`socket.timeout(ms).emit(event, payload, callback)` or
`emitWithAck`), the client answers with an **empty ack once the handler returns**. The ack is a receipt that the
event was handled. It carries no data.

| Situation | Ack |
|---|---|
| Handler returns normally | Sent after it returns |
| No handler registered for the event | Sent at once |
| Handler panics or the payload does not decode, without `WithEventErrorHandler` | Not sent. The session ends and the client reconnects |
| Same, with `WithEventErrorHandler` | Sent after the error handler runs. The session continues |
| The session ended while the handler ran | Dropped. An ack id means nothing on another connection |

Acknowledging an event that failed is what `WithEventErrorHandler` is for: a server that redelivers on a missing
ack would deliver the same failing payload again. A payload with binary attachments that fails to decode still
ends the session.

The client does not offer acknowledgements for its own emits, and acks the server sends are discarded.

---

## Liveness watchdog

The Engine.IO ping can take tens of seconds to notice a dead connection. If the server sends an application
heartbeat, check it yourself and call `Reconnect` when it goes silent. Reset the timestamp on every connect:
a timestamp left over from the previous session reads as silence before the first new heartbeat arrives.

```go
var lastBeat atomic.Int64 // unix nanos; 0 means no heartbeat yet on this session

client.OnConnect(func() { lastBeat.Store(0) })
client.On("machine::heartbeat", func() { lastBeat.Store(time.Now().UnixNano()) })

go func() {
	for range time.Tick(500 * time.Millisecond) {
		last := lastBeat.Load()
		if last != 0 && time.Since(time.Unix(0, last)) > 3*time.Second {
			lastBeat.Store(0)
			_ = client.Reconnect()
		}
	}
}()
```

A client that must come back quickly can also shorten the backoff, for example
`WithReconnectBackoff(200*time.Millisecond, 2*time.Second, 0.2)`.

---

## Stability

The client has been validated with:

- Integration tests against an in-process Socket.IO server, under the race detector
- Long-running connections and repeated server restarts against a real Socket.IO 4.8 server
- Memory and goroutine monitoring

See [docs/TESTING.md](docs/TESTING.md) for details and [CHANGELOG.md](CHANGELOG.md) for what changed in each version.

---

## Project Scope

This fork intentionally keeps a very small scope.

Supported:

- Client, root namespace
- WebSocket
- JSON events
- Automatic reconnect
- Empty acknowledgement of server events

Not supported:

- Socket.IO server
- Rooms
- Custom namespaces
- Acknowledgement callbacks with data, in either direction
- Binary payload API
- HTTP long-polling

---

## Why this fork?

The original project contains both client and server implementations and targets multiple use cases.

This fork removes unnecessary complexity and focuses exclusively on a reliable Socket.IO client for applications that need stable, long-lived connections.

---

## License

MIT
