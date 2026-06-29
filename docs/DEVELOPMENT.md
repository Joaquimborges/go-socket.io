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

## Plan phases

See [CLIENT_EVOLUTION_PLAN.md](./CLIENT_EVOLUTION_PLAN.md).

| PR | Scope |
|---|---|
| PR-1 | Remove server, examples, Redis/UUID deps |
| PR-2 | Minimal client (no namespaces/rooms/ACK) |
| PR-3 | v4 stability (EIO=4, PING→PONG, WebSocket default) |
| PR-4 | Automatic reconnect |
| PR-5 | Integration tests + soak + final README |

## Local tests

```bash
go mod tidy
go test ./... -race
```
