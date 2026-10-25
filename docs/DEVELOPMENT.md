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

## CI and releases

Three workflows in `.github/workflows/`, the same shape as the services' but adapted to a Go module. There is no
`develop` branch: every pull request targets `master`.

| Workflow | Runs |
|---|---|
| `tests.yml` | gofmt, `go mod tidy`, `go mod verify`, golangci-lint and `make test`. On every push to `master`, and as the pipeline's first stage |
| `pipeline.yml` | On every push to a pull request into `master`: tests → tag → release. Each stage waits for the previous one |
| `release.yml` | Publishes the tag it is given as the Latest GitHub release |

| Pull request | Tag pushed on each push | GitHub release |
|---|---|---|
| From any branch, e.g. `feat/client-hardening` | `v0.0.N-client-hardening` | None |
| From `release/X.Y.Z` | `vX.Y.Z-rc.N` | Latest |
| `release/X.Y.Z` merged | `vX.Y.Z` on the merge commit | Latest |

A branch tag lets a consumer try a pull request before it merges, with no `replace`:

```bash
go get github.com/Joaquimborges/go-socket.io@v0.0.1-client-hardening
```

To release:

1. Move the version's `CHANGELOG.md` entry from "unreleased" to its date, on `release/X.Y.Z`.
2. Open the pull request from `release/X.Y.Z` into `master`. `X.Y.Z` must be new and above every released version,
   or the pipeline fails. Each push publishes a new release candidate.
3. Merge it. The pipeline tags `vX.Y.Z` on the merge commit and publishes it as Latest.

**Tags never move and are never deleted.** This module is public: the Go module proxy and checksum database keep
the first content they see for a version, so a moved tag fails with a checksum mismatch for everyone who resolves
it afterwards. That is why a candidate is `-rc.N`, a new tag on every push, instead of the services' single `-rc`
tag moved with `git tag -f`. The final `vX.Y.Z` is needed because `go get @latest` skips pre-releases once any
release exists.
