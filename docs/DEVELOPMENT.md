# Development

## Phase workflow (PR)

Before starting **each new phase** of the evolution plan:

1. Update local `master` from remote:
   ```bash
   git checkout master
   git pull origin master
   ```
2. Create a **new branch** from `master`:
   ```bash
   git checkout -b feat/pr-N-short-description
   ```
3. Implement only the scope of the corresponding phase (PR).
4. Open a PR targeting `master` on `Joaquimborges/go-socket.io` (not upstream `feederco/go-socket.io`).

Phases that ship together in one version may share a branch, one commit per phase.

## Plan phases

See [CLIENT_EVOLUTION_PLAN.md](./CLIENT_EVOLUTION_PLAN.md).

| PR | Scope | Version |
|---|---|---|
| PR-1 | Remove server, examples, Redis/UUID deps | — |
| PR-2 | Minimal client (no namespaces/rooms/ACK) | — |
| PR-3 | v4 stability (EIO=4, PING→PONG, WebSocket default) | — |
| PR-4 | Automatic reconnect | — |
| Phase 0 | Per-session state, synchronous handshake, `CONNECT_ERROR` | v1.1.0 |
| Phase 1 | `Reconnect`, configurable backoff with jitter, event error handler | v1.1.0 |
| Phase 5 | Documentation and `CHANGELOG.md` | v1.1.0 |
| Phases 2–4 | Explicit ack, `EmitWithAck`, namespaces | Deferred |

## Local tests

```bash
go mod tidy
make test   # go test -v -race -count=1 ./...
```

The client tests use `fakeServer` from `client_integration_test.go`, an in-process Socket.IO server, so they need
no Node.js and no network. A new connection-level behavior gets a test there: the server's `accept` function
decides how each `CONNECT` is answered, and `fakeConn` exposes the packets the client sent.

Register handlers and callbacks before `Connect` in tests as well. Registering after it is a data race that
`-race` reports.

## Lint

CI runs `golangci-lint` v2.9.0, built with Go 1.26. With a newer local Go it fails on export data
(`export data version 4 is greater than maximum supported version 2`) before reading any code. Run it under the
CI toolchain:

```bash
GOTOOLCHAIN=go1.26.2 golangci-lint run
```

## Releases

Versions follow semver. A release gets an entry in [CHANGELOG.md](../CHANGELOG.md) and a tag on the `master`
merge commit, never on a feature branch.
