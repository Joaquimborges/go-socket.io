# Auditoria Técnica Completa — Cliente Socket.IO em Go

> **Nota (junho 2026):** este documento descreve o estado **original** do fork (baseado em googollee/go-socket.io com cliente mínimo acoplado ao servidor), **antes** do refactor descrito em [PLANO_EVOLUCAO_CLIENTE.md](./PLANO_EVOLUCAO_CLIENTE.md). Mantido como referência histórica e registo das decisões.
>
> **Estado atual da biblioteca:** cliente-only, Engine.IO v4, WebSocket, reconnect automático, `WithHeaders`. Ver [README.md](./README.md) para uso e API.

**Repositório:** `github.com/Joaquimborges/go-socket.io` (fork feederco/go-socket.io)  
**Escopo da auditoria:** código **pré-refactor**, uso como cliente contra servidor **Socket.IO v4** (Node.js)  
**Data da auditoria:** 27 de junho de 2026  
**Caso de uso:** conexão longa (dias), JSON, dezenas de eventos/minuto, Linux, alta disponibilidade

---

## Estado pós-refactor (resumo)

| Problema original (auditoria) | Resolução |
|---|---|
| Sem reconexão | Reconnect com backoff 1s → 30s |
| Engine.IO v3 (`EIO=3`) | Engine.IO v4 (`EIO=4`) |
| PING ignorado | PING → PONG em `engineio/client.go` |
| `Connect()` duplicado vazava goroutines | `Connect()` idempotente + `ErrAlreadyConnected` |
| Transporte default polling | Dial WebSocket-only |
| Race em `nextID()` / ACK | ACK removido da API; resposta interna a `NeedAck` |
| Zero testes do cliente | Testes unitários básicos; integração pendente |
| Callbacks na read goroutine | Mantido (documentado); handlers devem ser rápidos |

**Pendente:** testes de integração contra servidor Socket.IO v4 real e soak de longa duração.

---

## Sumário Executivo (estado original auditado)

Esta biblioteca é, em sua essência, uma implementação de **servidor** Socket.IO com um **cliente mínimo** adicionado posteriormente. O cliente funciona para cenários básicos contra servidores compatíveis com Engine.IO v3, mas apresenta **lacunas críticas** para produção de longa duração contra Socket.IO v4:

| Severidade | Problema |
|---|---|
| 🔴 Crítico | Sem lógica de reconexão |
| 🔴 Crítico | Engine.IO v3 hardcoded (`EIO=3`) — incompatível com Socket.IO v4 nativo |
| 🔴 Crítico | PING do servidor ignorado — conexão morre após `pingTimeout` |
| 🔴 Crítico | `Connect()` duplicado vaza goroutines e conexões |
| 🟠 Importante | Transporte padrão é polling HTTP (não websocket) |
| 🟠 Importante | Race condition em `nextID()` (ACK IDs) |
| 🟠 Importante | Zero testes automatizados para o cliente |
| 🟠 Importante | Callbacks de evento executam na goroutine de leitura (sem isolamento) |

**Veredicto:** **Não está pronta para produção** como cliente Socket.IO v4 de longa duração sem correções substanciais.

---

## 1. Arquitetura Geral

### 1.1 Visão em camadas

```
Application (seu código Go)
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
    │  serve() — goroutine de heartbeat (PING)
    │  NextReader() / NextWriter()
    ▼
transport.Conn                     ← polling ou websocket
    │  polling: HTTP long-poll (GET/POST) + payload.Payload
    │  websocket: gorilla/websocket + wrapper com mutex
    ▼
TCP / TLS
```

### 1.2 Fluxo de conexão

```
1. NewClient(addr, opts)
   └─ Parse URL, extrai namespace do path, monta URL /socket.io/{namespace}

2. Client.Connect()
   ├─ engineio.Dialer.Dial(url)          [EIO=3 hardcoded]
   │   └─ Tenta transports (default: polling apenas)
   │       ├─ HTTP GET → recebe pacote OPEN (sid, pingInterval, pingTimeout)
   │       └─ Inicia goroutines: getOpen, serveGet, servePost (polling)
   ├─ newConn(engineConn, handlers)
   ├─ connectClient() → envia pacote Socket.IO CONNECT (tipo 40)
   └─ go clientError / clientWrite / clientRead
```

**Arquivo:** `client.go:66-91`

### 1.3 Handshake Engine.IO

O dialer adiciona `EIO=3` à query string:

```go
// engineio/dialer.go:28-30
query := u.Query()
query.Set("EIO", "3")
```

O servidor responde com pacote OPEN contendo JSON:

```json
{"sid":"...","upgrades":["websocket"],"pingInterval":25000,"pingTimeout":20000}
```

Parsed em `transport.ReadConnParameters()`.

### 1.4 Handshake Socket.IO

Após Engine.IO conectado, `connectClient()` envia pacote CONNECT no namespace root:

```go
// client.go:264-284
header := parser.Header{Type: parser.Connect}
return c.encoder.Encode(header)  // wire: "40"
```

O servidor responde com `40` (ou `40{...}` com sid/auth). Handler: `clientConnectPacketHandler()`.

### 1.5 Leitura

```
clientRead() [1 goroutine]
  └─ loop infinito:
       decoder.DecodeHeader(&header, &event)  → engineio.NextReader() → transport
       switch header.Type:
         Connect    → clientConnectPacketHandler
         Disconnect → clientDisconnectPacketHandler
         Event      → eventPacketHandler → OnEvent callback
         Ack        → ackPacketHandler
```

### 1.6 Escrita

```
Emit(event, args...)
  └─ namespaceConn.Emit()
       └─ conn.write(header, args)  → writeChan (unbuffered)
            └─ clientWrite() [1 goroutine]
                 └─ encoder.Encode() → engineio.NextWriter() → transport
```

**Padrão correto:** uma goroutine de escrita serializa todas as escritas. Múltiplas goroutines podem chamar `Emit()` com segurança em relação ao websocket (desde que passem pelo `writeChan`).

### 1.7 Heartbeat

```
engineio/client.serve() [1 goroutine, criada no Dial]
  └─ loop:
       time.After(pingInterval)
       NextWriter(PING) → envia ping ao servidor
       SetWriteDeadline(...)
```

Resposta PONG do servidor → `NextReader()` atualiza `SetReadDeadline`.

### 1.8 Reconexão

**Não existe.** Nenhum arquivo implementa reconnect, retry ou backoff. Busca por `reconnect`, `Reconnect`, `retry` no código retorna apenas `retryError` interno do payload (transport upgrade), não reconexão de aplicação.

---

## 2. Concorrência

### 2.1 Respostas diretas

| Pergunta | Resposta |
|---|---|
| Existe apenas uma goroutine lendo? | **Sim** — `clientRead()` |
| Existe apenas uma goroutine escrevendo? | **Sim** — `clientWrite()` |
| Várias goroutines podem chamar `Emit()` simultaneamente? | **Sim** — via `writeChan` |
| Existe mutex protegendo a conexão? | **Parcial** — websocket tem `writeLocker`/`readLocker`; polling usa `payload.Payload` com CAS atômico |
| Existe channel de escrita? | **Sim** — `writeChan chan parser.Payload` (unbuffered) |
| Risco de "concurrent write to websocket"? | **Baixo** — serializado por `clientWrite` + mutex no wrapper |
| Risco de data race? | **Sim** — ver problemas abaixo |

### 2.2 Serialização de escrita (correto)

```go
// connection.go:115-131
func (c *conn) write(header parser.Header, args ...reflect.Value) {
    select {
    case c.writeChan <- pkg:    // unbuffered — bloqueia até clientWrite consumir
    case <-c.quitChan:
        return
    }
}
```

```go
// engineio/transport/websocket/wrapper.go:93-113
func (w wrapper) NextWriter(...) {
    w.writeLocker.Lock()        // mutex — impede WriteMessage concorrente
    ...
}
```

### 2.3 Data race: `nextID()` sem proteção

```go
// connection.go:109-113
func (c *conn) nextID() uint64 {
    c.id++    // ← SEM mutex, SEM atomic
    return c.id
}
```

Chamado de `namespaceConn.Emit()` quando há callback ACK:

```go
// namespace_conn.go:72-78
if lastV.Kind() == reflect.Func {
    header.ID = nc.conn.nextID()   // race se múltiplas goroutines emitem com ACK
    nc.ack.Store(header.ID, f)
}
```

**Cenário real:** duas goroutines chamam `Emit("event", data, ackFn)` simultaneamente → IDs duplicados → ACKs trocados ou perdidos.

### 2.4 Data race: `context` do namespace

```go
// namespace_conn.go:47-52
func (nc *namespaceConn) SetContext(ctx interface{}) {
    nc.context = ctx    // sem lock
}
```

Documentação afirma "no need to lock" assumindo acesso single-threaded, mas callbacks rodam na goroutine de leitura enquanto outras goroutines podem chamar `SetContext`/`Emit` concorrentemente.

### 2.5 Callbacks concorrentes com leitura

Comentário em `namespace_conn.go:14-16`:

> "The handlers are called in one goroutine, so no need to lock context"

**Verdade parcial:** event handlers rodam na goroutine `clientRead`, mas `Emit()` pode ser chamado de qualquer goroutine. Se um handler chamar `Emit()` e bloquear esperando ACK, e o ACK chegar na mesma goroutine de leitura → **deadlock clássico**.

**Cenário real:**

```go
client.OnEvent("request", func(c socketio.Conn) {
    c.Emit("response", data, func(result string) { ... })  // bloqueia writeChan
    // ACK precisa ser lido por clientRead — mesma goroutine → DEADLOCK
})
```

### 2.6 `errorChan` unbuffered

```go
// connection.go:134-139
func (c *conn) onError(namespace string, err error) {
    select {
    case c.errorChan <- newErrorMessage(namespace, err):
    case <-c.quitChan:
    }
}
```

Se `clientError` estiver executando um `OnError` lento, chamadas subsequentes a `onError` bloqueiam a goroutine chamadora (pode ser `clientWrite` ou `clientRead`).

---

## 3. Goroutines

### 3.1 Mapa completo (cliente conectado via polling)

| Goroutine | Criada por | Encerrada por | Risco de leak |
|---|---|---|---|
| `clientRead` | `Client.Connect()` | Erro de decode ou `quitChan` | Médio |
| `clientWrite` | `Client.Connect()` | `quitChan` | Baixo |
| `clientError` | `Client.Connect()` | `quitChan` | Baixo |
| `engineio.client.serve()` | `Dialer.Dial()` | `c.close` channel | Baixo |
| `polling.getOpen()` | `clientConn.Open()` | Retorno após FeedIn ou erro | Médio |
| `polling.serveGet()` | `clientConn.Open()` | Erro HTTP ou Close | Médio |
| `polling.servePost()` | `clientConn.Open()` | Erro HTTP ou Close | Médio |
| `rcWrapper/wcWrapper nagger` | Cada NextReader/NextWriter (ws) | Close do wrapper | Baixo |

### 3.2 Problema: `Connect()` duplicado

```go
// client.go:66-91 — NÃO verifica se já conectado
func (c *Client) Connect() error {
    enginioCon, err := dialer.Dial(c.url, nil)
    ...
    c.conn = newConn(enginioCon, c.handlers)  // substitui conn anterior
    go c.clientError()
    go c.clientWrite()
    go c.clientRead()
}
```

**Cenário real:** reconexão manual chamando `Connect()` novamente → 3 goroutines antigas + goroutines de polling antigas continuam vivas referenciando a conexão antiga → **leak de goroutines, leak de conexões TCP, mensagens duplicadas**.

### 3.3 Problema: triple `Close()` nos defers

Três goroutines (`clientRead`, `clientWrite`, `clientError`) têm:

```go
defer func() {
    if err := c.Close(); err != nil { ... }
}()
```

`closeOnce` protege contra double-close, mas qualquer goroutine que termina primeiro dispara `Close()` global, desconectando tudo. Comportamento correto mas frágil — se `clientRead` retorna por erro de decode, `clientWrite` e `clientError` também fecham via defer cascata.

### 3.4 Goroutines presas

- **`servePost`/`serveGet`:** se `Payload.FeedIn` retornar erro sem chamar `Close()`, a goroutine retorna mas outras podem ficar bloqueadas em `NextReader` esperando dados.
- **`clientWrite` bloqueado em `writeChan`:** se ninguém consome (bug), goroutine fica presa para sempre.
- **`time.After` em loops:** `engineio/client.serve()` e `payload.Payload` usam `time.After` em loops — timers acumulam até disparar (ver seção 9).

---

## 4. Channels

### 4.1 Inventário

| Channel | Buffer | Criado | Fechado | Produtor | Consumidor |
|---|---|---|---|---|---|
| `writeChan` | 0 | `newConn()` | **Nunca** | `conn.write()` | `clientWrite()` |
| `errorChan` | 0 | `newConn()` | **Nunca** | `conn.onError()` | `clientError()` |
| `quitChan` | 0 | `newConn()` | `conn.Close()` | — | 3 goroutines client |
| `engineio.client.close` | 0 | `Dialer.Dial()` | `client.Close()` | — | `serve()` |
| `payload.readerChan` | 0 | `payload.New()` | `Payload.Close()` | `FeedIn()` | `getReader()` |
| `payload.writerChan` | 0 | `payload.New()` | `Payload.Close()` | `FlushOut()` | `getWriter()` |
| `payload.readError` | 0 | `payload.New()` | **Nunca** | `putReader()` | `FeedIn()` |
| `payload.writeError` | 0 | `payload.New()` | **Nunca** | `putWriter()` | `FlushOut()` |

### 4.2 Deadlock potencial: Emit + ACK na mesma goroutine

Descrito na seção 2.5. O `writeChan` unbuffered bloqueia `Emit()` até `clientWrite` processar, mas se o caller é `clientRead` processando um evento, ninguém lê o ACK de resposta.

### 4.3 Bloqueio em `Emit()` durante shutdown

```go
select {
case c.writeChan <- pkg:
case <-c.quitChan:
    return
}
```

Se `clientWrite` já terminou mas `quitChan` ainda não foi lido por outra goroutine emitindo → **bloqueio indefinido** em `writeChan` (não há default/timeout).

### 4.4 Channels nunca fechados

`writeChan` e `errorChan` não são fechados em `Close()`. Dependem de `quitChan` para desbloquear via `select`. Funcional mas impede range/drain limpo.

---

## 5. Reconexão

### 5.1 Estado atual

**Reconexão não está implementada.** Análise completa:

| Critério | Status |
|---|---|
| Reconnect automático | ❌ Não existe |
| Recria estado corretamente | ❌ N/A |
| Limpa recursos antigos | ❌ `Connect()` duplicado não limpa |
| Pode abrir múltiplas conexões | ✅ Sim — bug |
| Deixa goroutines antigas vivas | ✅ Sim — bug |
| Callbacks registrados novamente | N/A — handlers persistem em `Client.handlers`, mas namespace conns são recriados |

### 5.2 O que acontece quando a conexão cai

1. `clientRead` recebe erro em `DecodeHeader` → log + `onError` + **return**
2. Defer chama `Close()` → `quitChan` fechado, disconnect handlers chamados
3. **`Client` fica inutilizável** — não há reconexão automática nem método `Reconnect()`
4. Aplicação deve criar novo `Client` manualmente

### 5.3 Implicação para longa duração

Para conexões de dias, quedas de rede, restart do servidor Node.js ou load balancer timeout **exigem lógica de reconexão na aplicação**. A biblioteca não oferece nenhuma abstração para isso.

---

## 6. Heartbeat

### 6.1 Mecanismo implementado

**Cliente inicia PING** (`engineio/client.go:105-135`):

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

**Cliente processa PONG** (`engineio/client.go:63-66`):

```go
case packet.PONG:
    c.conn.SetReadDeadline(time.Now().Add(c.params.PingInterval + c.params.PingTimeout))
```

### 6.2 BUG CRÍTICO: PING do servidor ignorado

O servidor (Session) responde corretamente a PING:

```go
// engineio/session/session.go:95-115 — SERVIDOR (referência correta)
case packet.PING:
    w, err := s.nextWriter(ft, packet.PONG)
    io.Copy(w, r)  // echo
    w.Close()
    r.Close()
```

Mas o **cliente** NÃO implementa isso:

```go
// engineio/client.go:62-82 — CLIENTE
switch pt {
case packet.PONG:
    SetReadDeadline(...)
case packet.CLOSE:
    Close(); return io.EOF
case packet.MESSAGE:
    return session.FrameType(ft), r, nil
}
// PING cai no default: fecha reader e continua loop SEM enviar PONG
if err = r.Close(); err != nil { ... }
```

**Socket.IO v4 / Engine.IO v4:** o servidor Node.js envia PING a cada `pingInterval` (default 25s). Se o cliente não responde com PONG dentro de `pingTimeout` (default 20s), o servidor **desconecta**.

**Cenário real:** conexão websocket estabelecida → servidor envia PING em 25s → cliente Go descarta → em 45s servidor fecha conexão → `clientRead` recebe EOF → conexão morta. Exatamente o oposto do necessário para "conexão de longa duração".

### 6.3 Detecção incorreta de conexão morta

- **Read deadline inicial nunca é setado** — só após receber primeiro PONG (resposta ao PING do cliente)
- Se o servidor só envia PING (não responde ao PING do cliente), o deadline nunca é atualizado via PONG
- Cliente pode achar que conexão está viva enquanto servidor já desconectou

### 6.4 `time.After` no loop de heartbeat

```go
case <-time.After(c.params.PingInterval):  // novo timer a cada iteração
```

Em conexão de semanas, acumula timers pendentes → pressão no GC (ver seção 9).

---

## 7. Uso do gorilla/websocket

### 7.1 Pontos positivos

- **Mutex de leitura/escrita** via `wrapper` — impede concurrent Read/Write
- **ReadCloser/WriteCloser wrappers** garantem unlock após Close()
- **Nag timer** (30s) alerta se Close() não foi chamado nos wrappers
- **SetWriteDeadline** protegido por mutex

### 7.2 Problemas

| Issue | Detalhe |
|---|---|
| Cliente usa polling por default | `client.go:68` — websocket nunca é usado a menos que configurado manualmente no Dialer |
| Sem SetReadDeadline inicial | Conexão websocket sem timeout de leitura até primeiro PONG |
| Upgrade não implementado no cliente | Cliente fica em polling mesmo se servidor oferece websocket |
| Buffer sizes default | `ReadBufferSize`/`WriteBufferSize` = 0 (default Go) — OK para JSON pequeno |

### 7.3 Concurrent write — mitigado

Toda escrita passa por `clientWrite` → `encoder.Encode` → `NextWriter` → `writeLocker.Lock()`. **Sem risco** de "concurrent write to websocket connection" desde que o fluxo não bypass o channel.

---

## 8. Segurança

### 8.1 Panics possíveis

```go
// client.go:95-96
func (c *Client) Close() error {
    return c.conn.Close()  // PANIC se conn == nil (antes de Connect())
}
```

```go
// handler.go:37, 42, 64 — panic em registro de handlers
panic("event handler must be a func.")
panic("handler function should be like func(socketio.Conn, ...)")
```

```go
// namespace_handler.go:109
msg = args[0].Interface().(string)  // panic se tipo errado
```

### 8.2 Nil pointer

- `Client.Close()` antes de `Connect()` → nil pointer dereference
- `Client.Emit()` antes de `Connect()` → log "not initialized", retorna silenciosamente (sem erro)

### 8.3 Acesso concorrente

- `namespaceConn.context` — sem proteção
- `conn.id` — sem proteção (ACK IDs)
- `namespaceHandler.events` — protegido por `RWMutex` ✅
- `namespaces` map — protegido por `RWMutex` ✅
- `ack sync.Map` — thread-safe ✅

### 8.4 Callbacks concorrentes

Event handlers executam na goroutine de leitura. Múltiplos eventos são processados **sequencialmente** (não paralelo), mas `Emit()` de outras goroutines é concorrente com handlers.

---

## 9. Memory Leak

### 9.1 Timers nunca parados

**`engineio/client.serve()`:**

```go
case <-time.After(c.params.PingInterval):  // leak clássico de time.After em loop
```

**`engineio/payload/payload.go` — `readTimeout()`/`writeTimeout()`:**

```go
return time.After(wait), true  // chamado em loops de select
```

Em conexão de semanas com polling ativo, centenas de timers pendentes acumulam memória até GC.

**Correção:** usar `time.NewTicker` ou `time.NewTimer` com `Stop()`/`Reset()`.

### 9.2 ACK callbacks órfãos

```go
// namespace_conn.go:78
nc.ack.Store(header.ID, f)
// Removido apenas em ackPacketHandler via defer nc.ack.Delete(header.ID)
```

Se ACK nunca chega (servidor não responde, conexão cai), entry permanece em `sync.Map` indefinidamente.

### 9.3 Goroutines órfãs

`Connect()` duplicado (seção 3.2) — principal fonte de acúmulo de goroutines.

### 9.4 Objetos acumulando

- `errorChan`/`writeChan` sem drain no Close
- Nag timer goroutines em websocket (2 por Read/Write operation) — terminam após 30s ou Close

---

## 10. API Pública

### 10.1 API atual

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

### 10.2 Problemas da API

| Problema | Impacto |
|---|---|
| Sem `context.Context` | Impossível cancelar Connect/Emit com timeout |
| `Connect()` bloqueante sem timeout | Pode bloquear indefinidamente em DNS/TCP |
| `Emit()` fire-and-forget | Sem retorno de erro, sem confirmação de entrega |
| `Close()` pode panic | Se chamado antes de Connect |
| Sem estado de conexão | Sem `IsConnected()`, `ConnectionState()` |
| Sem reconexão | Aplicação deve reimplementar tudo |
| `opts *engineio.Options` ignorado | `Client.opts` nunca usado em Connect |
| Transporte não configurável | Hardcoded polling.Default |
| Sem TLS config exposta | Precisa modificar código fonte |

### 10.3 API sugerida

```go
type Client interface {
    Connect(ctx context.Context) error
    Close() error
    Emit(ctx context.Context, event string, args ...any) error
    On(event string, handler EventHandler)
    ConnectionState() ConnectionState
    Done() <-chan struct{}  // sinaliza desconexão permanente
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

## 11. Compatibilidade com Socket.IO v4

### 11.1 Engine.IO

| Aspecto | Socket.IO v4 espera | Esta biblioteca | Compatível? |
|---|---|---|---|
| Protocol version | EIO=4 | EIO=3 hardcoded | ❌ |
| OPEN packet | JSON com maxPayload | Ignora maxPayload | ⚠️ |
| PING/PONG | Server ping → client pong | Client ping only, ignora server ping | ❌ |
| WebSocket transport | Preferido | Polling default, no upgrade | ⚠️ |
| Binary frames | Suportado | Suportado (não necessário) | ✅ |

### 11.2 Socket.IO protocol

| Aspecto | v4 | Esta biblioteca | Compatível? |
|---|---|---|---|
| CONNECT (40) | Com auth JSON opcional | Envia 40 sem auth | ⚠️ |
| EVENT (42) | JSON array `[event, ...args]` | Implementado | ✅ |
| ACK (43) | Com ID numérico | Implementado | ✅ |
| DISCONNECT (41) | Implementado | Implementado | ✅ |
| Binary events (45/46) | Suportado | Implementado | ✅ (não necessário) |
| Namespace | `/ns,` prefix | Implementado | ✅ |
| Protocol version header | v5 (implicit) | Não enviado | ⚠️ |

### 11.3 Teste prático esperado contra Socket.IO v4

```
1. Dial com EIO=3 → servidor v4 pode rejeitar (400 Bad Request) ou aceitar em modo legacy
2. Se aceitar → CONNECT funciona para namespace default
3. EVENT/ACK funciona para JSON simples
4. Heartbeat falha em ~25-45s → desconexão garantida
```

**README confirma:** "supports 1.4 version of the Socket.IO client" — não v4.

### 11.4 Diferenças críticas EIO v3 vs v4

- v4 adiciona `maxPayload` no OPEN
- v4 mudou formato de alguns pacotes
- Servidores Socket.IO v4 modernos podem desabilitar EIO v3 completamente
- v4 requer resposta a PING do servidor (heartbeat server-initiated)

---

## 12. Robustez (semanas de execução contínua)

### 12.1 Cenários de falha

| Cenário | Comportamento | Severidade |
|---|---|---|
| Queda de rede | Conexão morre, sem reconnect | 🔴 |
| Restart do servidor Node | Conexão morre, sem reconnect | 🔴 |
| Server PING timeout | Desconexão em ~45s | 🔴 |
| Load balancer idle timeout | Depende do transport; polling pode manter | 🟡 |
| `Connect()` acidental duplo | Leak de goroutines/conexões | 🔴 |
| ACK timeout | Entry permanece em sync.Map | 🟡 |
| `time.After` accumulation | Memória crescente lenta | 🟡 |
| Handler lento bloqueia leitura | Eventos subsequentes atrasam | 🟡 |
| Emit durante disconnect | Pode bloquear indefinidamente | 🟠 |
| HTTP Client timeout (polling, 1min) | Long-poll > 1min falha | 🟠 |

### 12.2 HTTP Client timeout no polling

```go
// engineio/transport/polling/transport.go:19-23
var Default = &Transport{
    Client: &http.Client{
        Timeout: time.Minute,  // 60 segundos max por request
    },
}
```

Se `pingInterval + pingTimeout > 60s` (configurável no servidor), long-poll GET expira → conexão cai.

---

## 13. Melhorias por Prioridade

### 🔴 Críticas (corrigir antes de produção)

1. **Implementar resposta a PING do servidor** — adicionar `case packet.PING` em `engineio/client.go:NextReader()` espelhando `session/session.go`
2. **Alterar EIO=3 para EIO=4** — `engineio/dialer.go:29` + validar parsing de OPEN
3. **Implementar reconexão** — backoff exponencial, re-handshake, re-CONNECT namespace
4. **Proteger `Connect()` contra chamada dupla** — retornar erro ou fechar conexão anterior
5. **Proteger `Client.Close()` contra nil** — `if c.conn != nil`
6. **Usar websocket como transporte default** — não polling para conexões longas
7. **Corrigir race em `nextID()`** — `atomic.AddUint64`

### 🟠 Importantes (recomendadas)

8. Adicionar `context.Context` em Connect/Emit/Close
9. Bufferizar `writeChan` (ex: 256) para evitar bloqueio de produtores
10. Executar event handlers em goroutine separada (worker pool) para evitar deadlock ACK
11. Substituir `time.After` por `time.NewTicker` em loops de heartbeat
12. Adicionar `IsConnected()` e canal `Disconnected()`
13. Expor configuração de transporte e TLS via `Client.opts`
14. Timeout em `Emit()` com context
4. Limpar ACKs órfãos com timer
15. Testes de integração cliente ↔ servidor Socket.IO v4 real
16. Graceful shutdown: drenar `writeChan` antes de fechar

### 🟢 Opcionais (refatorações)

17. Interface `Client` para testabilidade
18. Structured logging (slog) em vez de logger custom
19. Métricas (prometheus): connected, reconnects, events/s, errors
20. Suporte a auth no pacote CONNECT (Socket.IO v4)
21. Upgrade polling → websocket no cliente
22. Documentação específica para uso como cliente
23. Remover dependência de rooms/broadcast no cliente (dead code para caso de uso)

---

## 14. Evidências Detalhadas

### E1 — PING do servidor ignorado (CRÍTICO)

**Arquivo:** `engineio/client.go`  
**Função:** `(*client).NextReader()`  
**Linhas:** 55-82

**Fluxo:**
1. Servidor envia Engine.IO packet PING
2. `NextReader()` recebe via transport
3. Switch não tem case `packet.PING`
4. Cai no final: `r.Close()` e loop continua
5. Servidor nunca recebe PONG
6. Após `pingTimeout`, servidor fecha conexão

**Por que existe:** O código do cliente foi escrito assumindo heartbeat unidirecional (cliente inicia PING via `serve()`), copiando parcialmente o padrão do servidor (`session.go`) mas omitindo o handler de PING entrante.

**Correção proposta:**

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

### E2 — EIO=3 hardcoded (CRÍTICO)

**Arquivo:** `engineio/dialer.go`  
**Função:** `(*Dialer).Dial()`  
**Linhas:** 28-30

```go
query.Set("EIO", "3")
```

**Fluxo:** Toda conexão envia `?EIO=3&transport=polling`. Servidor Socket.IO v4 espera `EIO=4`.

**Cenário real:** Deploy contra servidor Socket.IO v4 strict → HTTP 400 "Unsupported protocol version" → `Connect()` retorna erro → aplicação nunca conecta.

**Correção:** `query.Set("EIO", "4")` + testes contra servidor v4 real. Considerar tornar configurável.

---

### E3 — Connect() duplicado vaza goroutines (CRÍTICO)

**Arquivo:** `client.go`  
**Função:** `(*Client).Connect()`  
**Linhas:** 66-91

**Fluxo:**
1. Primeira `Connect()` → cria conn1 + 3 goroutines + goroutines polling
2. Segunda `Connect()` → cria conn2, sobrescreve `c.conn`
3. Goroutines de conn1 continuam rodando, referenciando conn1 (orphan)
4. conn1 nunca é fechada explicitamente

**Cenário real:** Lógica de reconexão na aplicação chama `Connect()` novamente → após N reconexões, N×3 goroutines socketio + N×3 goroutines polling acumuladas → OOM ou file descriptor exhaustion.

**Correção:**

```go
func (c *Client) Connect() error {
    if c.conn != nil {
        if err := c.Close(); err != nil {
            return fmt.Errorf("close previous connection: %w", err)
        }
    }
    // ... resto do dial
}
```

---

### E4 — Client.Close() nil panic (CRÍTICO)

**Arquivo:** `client.go`  
**Função:** `(*Client).Close()`  
**Linhas:** 95-97

```go
func (c *Client) Close() error {
    return c.conn.Close()
}
```

**Cenário real:** `defer client.Close()` antes de `Connect()` em error path → panic → crash do processo.

**Correção:**

```go
func (c *Client) Close() error {
    if c.conn == nil {
        return nil
    }
    return c.conn.Close()
}
```

---

### E5 — Race condition nextID() (IMPORTANTE)

**Arquivo:** `connection.go`  
**Função:** `(*conn).nextID()`  
**Linhas:** 109-113

**Cenário real:** 10 goroutines emitem eventos com callback ACK simultaneamente → IDs 5 e 5 atribuídos → ACK do evento A entrega resposta ao callback do evento B.

**Correção:**

```go
func (c *conn) nextID() uint64 {
    return atomic.AddUint64(&c.id, 1)
}
```

---

### E6 — Deadlock Emit+ACK no handler (IMPORTANTE)

**Arquivo:** `namespace_conn.go` + `client.go`  
**Funções:** `Emit()` → `write()` → `clientRead()` → handler

**Cenário real:**

```go
client.OnEvent("getData", func(c socketio.Conn) {
    done := make(chan struct{})
    c.Emit("fetch", id, func(data Data) {
        process(data)
        close(done)
    })
    <-done  // espera ACK
})
// clientRead está bloqueada em <-done
// clientWrite precisa enviar "fetch" mas ACK response precisa ser lida por clientRead
// DEADLOCK
```

**Correção:** Executar handlers em goroutine separada:

```go
go func() {
    err := handler.dispatchEvent(conn, event, args...)
    ...
}()
```

Ou documentar explicitamente que callbacks ACK não podem ser usados dentro de handlers de evento.

---

### E7 — Transporte polling default (IMPORTANTE)

**Arquivo:** `client.go`  
**Linhas:** 67-69

```go
dialer := engineio.Dialer{
    Transports: []transport.Transport{polling.Default},
}
```

**Cenário real:** Conexão longa via HTTP polling → overhead de HTTP headers a cada evento (POST) e long-poll (GET) → latência maior, mais CPU, mais vulnerável a proxy timeouts vs websocket single connection.

**Correção:**

```go
dialer := engineio.Dialer{
    Transports: []transport.Transport{
        websocket.Default,
        polling.Default,
    },
}
```

E implementar upgrade no cliente.

---

### E8 — opts ignorado (IMPORTANTE)

**Arquivo:** `client.go`  
**Campo:** `opts *engineio.Options` (linha 26)

`Connect()` cria Dialer local sem usar `c.opts`. Configurações de PingInterval, PingTimeout, Transports, TLS são ignoradas.

**Correção:** Usar `c.opts` para configurar Dialer e transportes.

---

### E9 — Zero testes do cliente

**Evidência:** `grep -r "NewClient" *_test.go` → zero resultados.

Toda a cobertura de testes (que passa com `-race`) é do **servidor** e engineio interno. O cliente não tem nenhum teste unitário ou de integração.

---

### E10 — time.After leak (IMPORTANTE)

**Arquivo:** `engineio/client.go:116`, `engineio/payload/payload.go:274,287`

**Cenário real:** Conexão ativa por 30 dias, pingInterval=25s → ~103.000 timers criados e pendente até fire → pressão de GC, RSS crescente.

**Correção:**

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

## 15. Classificação Final

| Categoria | Nota (0-10) | Justificativa |
|---|---|---|
| **Arquitetura** | 5/10 | Separação read/write/error em goroutines é correta, mas cliente é afterthought sobre codebase de servidor |
| **Concorrência** | 4/10 | Serialização de escrita OK, mas races em nextID, deadlock ACK, callbacks na read goroutine |
| **Reconexão** | 0/10 | Inexistente; Connect() duplicado piora |
| **Robustez** | 2/10 | Heartbeat incompleto mata conexão; sem recovery; leaks em timers e goroutines |
| **Legibilidade** | 6/10 | Código Go idiomático, espelha server patterns, mas cliente subdocumentado |
| **Segurança** | 5/10 | Sem panics em paths comuns (exceto Close nil), reflection handlers com recover |
| **Prontidão para Produção** | **2/10** | Não recomendado para Socket.IO v4 long-running sem correções críticas |

### Nota Geral: **3/10**

---

## Conclusão (auditoria original)

### Pronta para produção? **NÃO** *(na data da auditoria)*

Após o refactor (PRs 1–4 + headers), a biblioteca endereça os itens críticos abaixo. Validação final depende de testes de integração e soak.

### O que precisava ser corrigido (checklist histórico):

1. ✅ Responder PING do servidor (sem isso, conexão morre em segundos/minutos)
2. ✅ Migrar para Engine.IO v4 (`EIO=4`)
3. ✅ Implementar reconexão com backoff e cleanup de recursos
4. ✅ Usar websocket como transporte principal
5. ✅ Proteger contra Connect() duplicado e Close() nil
6. ✅ Corrigir race condition em ACK IDs *(ACK removido da API pública)*
7. ⏳ Adicionar testes de integração contra Socket.IO v4 real
8. ✅ Resolver deadlock Emit+ACK em event handlers *(OnConnect assíncrono + fix Encode)*

### Caminho alternativo *(contexto da auditoria original)*

Se correções na biblioteca não são viáveis a curto prazo, considere:

- **`github.com/maldikhan/go.socket.io`** ou **`github.com/feederco/go-socket.io`** (verificar se fork tem fixes)
- **Cliente Socket.IO via sidecar Node.js** com comunicação gRPC/HTTP
- **Implementar cliente mínimo custom** usando gorilla/websocket + protocolo Socket.IO v4 (~500 linhas para caso de uso simples)

Para o caso de uso descrito (JSON, longa duração, estabilidade), um cliente websocket dedicado com heartbeat correto e reconnect seria significativamente mais confiável do que esta biblioteca em seu estado atual.

---

## Apêndice A — Mapa de arquivos relevantes

| Arquivo | Papel |
|---|---|
| `client.go` | API pública do cliente Socket.IO |
| `connection.go` | Conn interna, channels, write/read |
| `connection_handlers.go` | Handlers de pacotes (connect/event/ack/disconnect) |
| `namespace_conn.go` | Emit, ACK, namespace |
| `namespace_handler.go` | Registro e dispatch de callbacks |
| `parser/encoder.go` | Codificação Socket.IO protocol |
| `parser/decoder.go` | Decodificação Socket.IO protocol |
| `engineio/dialer.go` | Dial Engine.IO (EIO version) |
| `engineio/client.go` | Cliente Engine.IO (heartbeat, NextReader/Writer) |
| `engineio/session/session.go` | Referência correta de heartbeat (servidor) |
| `engineio/transport/polling/connect.go` | Cliente HTTP long-polling |
| `engineio/transport/websocket/wrapper.go` | Mutex gorilla/websocket |
| `engineio/transport/websocket/transport.go` | Dial websocket |
| `engineio/payload/payload.go` | Encode/decode payload polling |

## Apêndice B — Testes executados

```
go test ./... -count=1     → OK (sem testes de cliente)
go test ./... -race        → OK (race detector limpo nos testes existentes)
```

**Nota:** Race detector limpo nos testes **não cobre** o código do cliente, que não possui testes.
