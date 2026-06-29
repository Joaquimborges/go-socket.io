# Architecture

The library intentionally follows a minimal design.

```
Application
      │
      ▼
 Client
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
- Small public API
- No unnecessary abstractions

The implementation favors simplicity and maintainability over feature completeness.

## Public API

- NewClient()
- Connect()
- Close()
- Emit()
- On()
- OnConnect()
- OnDisconnect()

## Internal loops

The client maintains only two goroutines during normal operation:

- readLoop
- writeLoop

Reconnect is managed internally and does not create additional permanent goroutines.

## Non-goals

This project intentionally does not implement:

- Socket.IO server
- Rooms
- Multiple namespaces
- ACK callbacks
- Binary payload API