# Stability Validation

This document describes the validation performed before each version of this client was considered ready for
production. The most recent version comes first.

---

# v1.1.0 — October 2026

## Automated tests

`make test` runs every package under the race detector. The client tests run against `fakeServer`
(`client_integration_test.go`), an in-process server built on gorilla/websocket that speaks Engine.IO v4 and
Socket.IO v5. It answers `CONNECT` per test and lets a test inject what a real server cannot be made to do on
demand: a refusal, a handshake that never completes, a write failure while a handler runs.

| Test | What it proves |
|---|---|
| `TestClientConnectEmitAndReceive` | Handshake, emit and a typed handler |
| `TestClientAcksServerEventWithEmptyReceipt` | The receipt is exactly `3<id>`, for handled and unhandled events |
| `TestClientConnectErrorIsReportedAndRetried` | `CONNECT_ERROR` reaches `OnReconnectAttempt` as `*ConnectError`, no `OnConnect`, transport dropped, retried |
| `TestClientDropsAckOfEndedSession` | An ack whose session ended never reaches the next connection, and only one read loop is left |
| `TestClientReconnectsAfterServerClosesTransport` | Transport loss → `OnDisconnect` → reconnect |
| `TestClientCloseDuringHandshake` | `Close` while waiting for `CONNECT` stops the loop with no retry |
| `TestClientReconnectRequested` | `Reconnect` ends the session with `ErrReconnectRequested` and connects again |
| `TestClientBackoffSequence` | The reported waits follow `WithReconnectBackoff` (50, 100, 200, 200 ms) |
| `TestClientEventErrorHandlerKeepsSession` | A panic and an undecodable payload are acknowledged and the session continues |
| `TestClientHandlerPanicEndsSessionByDefault` | Without the option, the previous behavior: no ack, session ends |

`TestClientDropsAckOfEndedSession` was checked by mutation: with the v1.0.0 ack path restored in a copy of the
code, the stale ack (`37`) reached the new connection and the test failed.

Also run for this version: `golangci-lint` 2.9.0 (0 issues), `gofmt -s`, `go mod tidy` (no change), and the
`claw-machine-api` socket package built and tested against the branch.

## Soak against the relay

Environment:

- `machine-socket-server-ts` (Socket.IO 4.8.1) at commit `1eec151`, compiled outside its repository and run
  with a throwaway Redis
- `MACHINE_HEARTBEAT_MODE=enforce`: a heartbeat every 500 ms, a machine silent for 2 s is disconnected
- The relay restarted every 15 s (down about 1 s each time), then left up for a stable period
- Go 1.27.1, macOS arm64

Clients, all in one process:

- **medusa** — authenticates as the backend and receives the relay's facts through `emitWithAck`
- **machine** — authenticates as a machine with `x-heartbeat: 1` and answers every heartbeat
- **badkey** — a wrong key; must never connect
- **fastmach** (second run only) — another machine with `WithReconnectBackoff(200ms, 2s, 0.2)`,
  `WithEventErrorHandler` and a watchdog that calls `Reconnect` every 20 s and on 3 s of heartbeat silence

### Run 1 — default options, 100 restarts + 10 min stable (38 min)

| Measure | Result |
|---|---|
| Reconnects (medusa, machine) | 101 disconnects, 101 reconnects each: exactly one per restart |
| Goroutines | 9–11 throughout, 2 after `Close` |
| Memory | Alloc 0.6 → 1.7 MB, flat through the stable period; Sys 13.4 → 18.9 MB |
| Facts acknowledged (`emitWithAck`, 5 s timeout) | 153 of 153, no timeout |
| Heartbeats answered | 3961, no `heartbeat lost` on the relay |
| badkey | 76 `*ConnectError` carrying the relay's message, never connected |
| Library error logs | None |

### Run 2 — default and machine profile, 30 restarts + 5 min stable (14 min)

| Measure | Result |
|---|---|
| Downtime per drop, default options | 3.01 s average (relay down plus the 1 s initial backoff) |
| Downtime per drop, machine profile | 1.05 s average over 91 drops, 3.0 s maximum during relay restarts |
| `Reconnect` calls (fastmach) | 60, each followed by exactly one `OnDisconnect(ErrReconnectRequested)` |
| Facts acknowledged | 183 of 183, no timeout |
| Heartbeats answered | 1442 (machine) and 1407 (fastmach), no `heartbeat lost` |
| Goroutines | Peak 15 with four clients, not growing |
| Library error logs | None |

23 of the 60 `Reconnect` calls came from a flaw in the test's watchdog, not the library: it kept the previous
session's heartbeat timestamp, so a relay restart longer than 3 s read as silence right after reconnecting. The
README's watchdog example resets the timestamp on connect for that reason.

The faster machine reconnect regularly came back before medusa did, so the relay logged
`no medusa connected` 43 times. That is the case its `reportConnectedMachines` covers by reporting again when
the backend connects.

### Reproducing

The soak driver and the scripts are not in this repository. To repeat it: compile the relay with `tsc --outDir`
to a scratch directory and run it with `NODE_PATH` pointing at its `node_modules`, a local Redis, and
`PERSEUS_CLIENT_KEY` / `SMALL_TOY_ACCESS_KEY` / `SMALL_TIGER_ACCESS_KEY` set to test keys. Connect the clients
above with the headers the relay's `middleware/auth.ts` expects (`x-service`, `x-client-key`, `x-machine`,
`x-heartbeat`), and restart the relay in a loop. `cmd/soak-until-100.sh` and `cmd/server-churn.sh` do the
restarts, but `cmd/testclient` sends an `Authorization` header that the relay rejects.

---

# v1.0.0 — June 2026

## Environment

- Socket.IO v4
- Engine.IO v4
- WebSocket transport
- Go 1.24
- Node.js test server

---

## Test 1 — Long-running connection

Duration:

- 8+ hours

Traffic:

- One event every 30 seconds
- Bidirectional communication

Observed:

- Stable memory usage
- Stable goroutine count
- No unexpected reconnects
- No resource leaks

Final state:

```
Connected: true
Reconnects: 0

Messages Sent: 938
Messages Received: 938

Memory Alloc: ~0.6 MB
Memory Sys: ~13.5 MB

Goroutines: 6

Active readLoops: 1
Active writeLoops: 1
```

---

## Test 2 — Reconnect stress test

Server restarted continuously.

Results:

- 100 successful reconnects
- No goroutine leaks
- No memory growth
- No stale read/write loops

Final report:

```
Reconnects: 100

Goroutines: 5

Active readLoops: 1
Active writeLoops: 1

Memory Alloc: ~0.7 MB
Memory Sys: ~13.6 MB
```

---

## Race Detector

Executed:

```bash
go test ./... -race
```

Result:

```
PASS
```

No race conditions detected.

---

## Leak Detection

During development a goroutine leak was identified.

Cause:

- writeLoop remained blocked after reconnect.

Fix:

- writeLoop now exits when sessionEnd is closed.

The reconnect stress test confirmed the fix after 100 reconnects.

---

## Conclusion

The client was validated for:

- Long-lived connections
- Automatic reconnect
- Stable memory usage
- Stable goroutine count
- Race-free execution

These tests provide confidence for production use in backend applications requiring persistent Socket.IO connections.