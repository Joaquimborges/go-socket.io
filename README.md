# go-socket.io

A lightweight and production-oriented Socket.IO v4 client for Go.

This repository is a focused fork of the original `feederco/go-socket.io`, redesigned to provide a simple and reliable Socket.IO client for backend applications.

The project intentionally focuses on:

- Socket.IO v4
- Engine.IO v4
- WebSocket transport
- Automatic reconnect
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

    socketio "github.com/Joaquimborges/go-socket.io"
)

type Message struct {
    Username string `json:"username"`
    Message  string `json:"message"`
}

func main() {
    client := socketio.NewClient("ws://localhost:8083/socket.io")

    client.On("show_message", func(msg Message) {
        log.Printf("%s: %s", msg.Username, msg.Message)
    })

    client.OnConnect(func() {
        log.Println("connected")
    })

    client.OnDisconnect(func(err error) {
        log.Println("disconnected:", err)
    })

    if err := client.Connect(); err != nil {
        log.Fatal(err)
    }

    client.Emit("show_message", Message{
        Username: "Pedro",
        Message:  "Hello!",
    })

    select {}
}
```

---

## API

```go
socketio.NewClient()

client.Connect()

client.Close()

client.Emit(event, payload)

client.On(event, handler)

client.OnConnect(handler)

client.OnDisconnect(handler)
```

---

## Features

- Socket.IO v4
- Engine.IO v4
- WebSocket transport
- Automatic reconnect with exponential backoff
- Long-lived connections
- JSON encoding/decoding
- Typed event handlers
- Graceful disconnect
- Production-oriented API

---

## Stability

The client has been validated with:

- Continuous long-running connections
- Automatic reconnect
- Forced reconnect stress tests
- Memory monitoring
- Goroutine leak detection
- Go race detector

See **docs/TESTING.md** for details.

---

## Project Scope

This fork intentionally keeps a very small scope.

Supported:

- Client
- WebSocket
- JSON events
- Automatic reconnect

Not supported:

- Socket.IO server
- Rooms
- Namespaces
- ACK callbacks
- Binary payload API

---

## Why this fork?

The original project contains both client and server implementations and targets multiple use cases.

This fork removes unnecessary complexity and focuses exclusively on a reliable Socket.IO client for backend services that need stable, long-lived connections.

---

## License

MIT