# Plano do Fork — Cliente Socket.IO Simples

Fork para um backend Go que precisa de cliente Socket.IO v4 estável e minimalista.

**Filosofia:** menos arquivos, menos código, menos abstrações.

**Referências:** [AUDITORIA_CLIENTE.md](./AUDITORIA_CLIENTE.md) · [README.md](./README.md)

---

## Estado atual (junho 2026)

| Fase | Status |
|---|---|
| Remoção do servidor e deps legadas | ✅ Concluído |
| Cliente mínimo (`Client` único, sem namespaces/rooms/ACK) | ✅ Concluído |
| Engine.IO v4, PING→PONG, WebSocket-only | ✅ Concluído |
| Reconnect automático com backoff | ✅ Concluído |
| Headers HTTP no dial (`WithHeaders`) | ✅ Concluído |
| Testes de integração + soak | ⏳ Pendente |

**Próximo passo:** testes de integração contra servidor Socket.IO v4 real (`testdata/`) e soak local antes de deploy em produção.

---

## Objetivo

- Conectar ao Socket.IO v4 (Node.js)
- Permanecer conectado dias
- Reconectar automaticamente (backoff 1s → … → cap 30s)
- Emitir e receber JSON
- Responder heartbeat (PING → PONG)
- Enviar headers HTTP no handshake (auth, etc.)

**Sem ACK** — backend não usa callback de resposta em nenhum evento.

---

## API pública

```go
client, err := socketio.NewClient(url,
    socketio.WithHeaders(http.Header{
        "Authorization": {"Bearer <token>"},
    }),
)

client.On("machine_connected", func(data MachineConnected) { ... })

client.OnConnect(func() { log.Println("connected") })       // toda conexão ok, incl. reconnect
client.OnDisconnect(func(err error) { log.Println("down", err) })

err := client.Connect()
if err := client.Emit("show_message", map[string]any{"username": "pedro"}); err != nil {
    // err == socketio.ErrNotConnected se offline
}
client.Close()
```

| Método | Descrição |
|---|---|
| `NewClient(url, opts...)` | Cria o client |
| `WithHeaders(h)` | Headers HTTP em cada dial (inclui reconnect) |
| `On(event, handler)` | Eventos Socket.IO |
| `OnConnect()` | Conexão estabelecida (1ª vez e após cada reconnect) |
| `OnDisconnect(err)` | Transporte caiu, antes do backoff |
| `Connect()` | Inicia loops + reconnect |
| `Emit(...)` | Envia JSON; retorna `error` (`ErrNotConnected` se offline) |
| `Close()` | Para reconnect e fecha |

**Rejeitado:** `OnReconnect` separado (redundante com `OnConnect`).

**Rejeitado:** ACK / `Emit` com callback.

**Erros exportados:** `ErrEmptyAddr`, `ErrNotConnected`, `ErrAlreadyConnected`.

---

## Arquitetura

```
App → Client (único struct exportado) → engineio → websocket
              ↓
           parser
```

- **Um `Client` struct** — campos privados, loops em `client.go` + helpers no mesmo pacote.
- **Sem** struct `connection` separada (evita fachada pass-through).
- **`engineio/` + `parser/`** — subpacotes mantidos (protocolo já existe).

```
readLoop  → parser → On() handler (síncrono)
Emit      → writeChan → writeLoop → engineio
```

- **2 goroutines** (read + write). Erros fatais saem do readLoop.
- **`connectOnce()`** interno — reconnect chama isso, não `Connect()` público.

---

## Sem ACK — o que foi removido

| Removido | Motivo |
|---|---|
| `ack sync.Map`, handlers ACK | Sem callbacks de resposta |
| `nextID()`, branch ACK em `Emit` | IDs de ACK desnecessários |
| Case `parser.Ack` no readLoop | Pacotes ACK do servidor são respondidos internamente quando `NeedAck` |

**Parser:** mantém tipos `Ack` no decoder (protocolo existe); a API pública não expõe callbacks.

---

## Namespace root — validar, não ignorar

Cliente suporta **apenas** namespace default (`""` ou `"/"` no wire).

| `header.Namespace` | Ação |
|---|---|
| `""` | OK — root implícito (`42[...]`) |
| `"/"` | OK |
| qualquer outro (`/admin`, `/foo`) | **Warn + discard** — não dispatch handler |

Emit sempre com namespace root. Se o servidor emitir noutro namespace, o log aparece imediatamente.

---

## Princípio limpeza (futuro)

```
integration (connect, emit, receive, reconnect)
    → soak
    → remover polling / buffer.go / dead code (opcional)
```

---

## Histórico de fases (concluídas)

As fases abaixo documentam a evolução do fork. A API estável está descrita no [README](./README.md).

<details>
<summary>PR-1 — Remover servidor</summary>

- Removidos `server.go`, Redis, UUID, `_examples/`, código server-side em `engineio/`
</details>

<details>
<summary>PR-2 — Cliente mínimo</summary>

- Um `Client` struct; `On`, `OnConnect`, `OnDisconnect`, `Emit`, `Connect`, `Close`
- Removidos namespaces, rooms, broadcast, ACK
</details>

<details>
<summary>PR-3 — Estabilidade v4</summary>

- `EIO=4`, PING→PONG, dial WebSocket-only, `Connect()` idempotente
</details>

<details>
<summary>PR-4 — Reconnect</summary>

- Backoff exponencial (1s → 30s), `Close()` cancela loop
</details>

<details>
<summary>Extra — WithHeaders</summary>

- Option `WithHeaders(http.Header)` para auth e headers custom no handshake
</details>

---

## Pendente

### Testes + soak

- `testdata/socketio-v4-server/` — servidor Node mínimo para CI
- Integration: connect, emit, receive, reconnect (sem ack)
- Soak 4h local; 24h recomendado antes de deploy em prod

**Opcional (higiene):** remover `polling/`, `payload/`, `parser/buffer.go` após soak OK.

---

## Definition of Done

- [x] Sem código servidor
- [x] Sem ACK na API pública (emit/receive)
- [x] API: `NewClient`, `Connect`, `Emit`, `On`, `OnConnect`, `OnDisconnect`, `Close`
- [x] WebSocket + EIO=4 + PING→PONG
- [x] Reconnect automático
- [x] Headers HTTP no dial
- [ ] Integration tests (sem ack)
- [ ] Soak estável
- [x] `go test ./... -race` limpo

---

## Fora do escopo (permanece)

FSM, options extensas além de `WithHeaders`, Context na API, handlers async, rooms, broadcast, servidor, upgrade polling, namespaces custom.

---

**Workflow de desenvolvimento:** ver [DEVELOPMENT.md](./DEVELOPMENT.md).
