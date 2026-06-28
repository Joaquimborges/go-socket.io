# go-socket.io

Cliente [Socket.IO](https://socket.io) v4 em Go para backends que mantêm uma conexão WebSocket persistente com um servidor Socket.IO (tipicamente Node.js).

Fork de [googollee/go-socket.io](https://github.com/googollee/go-socket.io), reduzido a **cliente only**: uma API pequena, reconnect automático e foco em conexões de longa duração.

## O que é

Esta biblioteca implementa o protocolo **Engine.IO v4 + Socket.IO** do lado cliente. O teu serviço Go conecta-se ao servidor, recebe eventos (`On`), envia eventos (`Emit`) e reconecta sozinho quando o transporte cai.

**Caso de uso típico:** um worker ou microserviço Go que fica ligado horas ou dias a um servidor Socket.IO, trocando JSON em eventos nomeados (ex.: `machine_connected`, `ping`, `show_message`).

## O que suporta

| Suportado | Não suportado |
|---|---|
| WebSocket + JSON | Servidor Socket.IO |
| Namespace root (`/` ou `""`) | Rooms, broadcast, namespaces custom |
| `Emit` / `On` de eventos | ACK / callbacks de resposta |
| Reconnect com backoff (1s → … → 30s) | Payloads binários na API pública |
| Heartbeat PING → PONG | Upgrade polling → WebSocket |
| Headers HTTP no handshake (`WithHeaders`) | |

**Requisitos:** Go 1.26+, servidor Socket.IO **v4**, transporte WebSocket.

## Instalação

```bash
go get github.com/Joaquimborges/go-socket.io
```

```go
import socketio "github.com/Joaquimborges/go-socket.io"
```

## Uso básico

```go
package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	socketio "github.com/Joaquimborges/go-socket.io"
)

func main() {
	client, err := socketio.NewClient("http://localhost:8083")
	if err != nil {
		log.Fatal(err)
	}

	client.On("ping", func(data any) {
		log.Println("ping:", data)
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

	if err := client.Emit("show_message", map[string]any{
		"username": "pedro",
	}); err != nil {
		if errors.Is(err, socketio.ErrNotConnected) {
			log.Println("offline, evento não enviado")
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	_ = client.Close()
}
```

## Autenticação e headers

Headers são enviados em **cada** tentativa de conexão, incluindo reconnect:

```go
import "net/http"

client, err := socketio.NewClient("https://api.example.com",
	socketio.WithHeaders(http.Header{
		"Authorization": {"Bearer <token>"},
	}),
)
```

## Ciclo de vida

```
Connect()  →  dial WebSocket  →  OnConnect()
     ↑                              │
     │                         readLoop / writeLoop
     │                              │
     └── backoff ← OnDisconnect ← sessão cai
```

- **`Connect()`** — idempotente; inicia os loops e o reconnect. Chamada duplicada retorna `ErrAlreadyConnected`.
- **`OnConnect()`** — dispara na primeira conexão **e** após cada reconnect bem-sucedido.
- **`OnDisconnect(err)`** — dispara quando a sessão termina, **antes** do backoff. O client tenta reconectar sozinho até `Close()`.
- **`Close()`** — cancela reconnect e fecha o transporte.

Handlers registados com `On()` são **síncronos** e correm na goroutine de leitura. Evita bloqueios longos dentro do handler.

## API

| Símbolo | Descrição |
|---|---|
| `NewClient(url, opts...)` | Cria o client; normaliza o path para `/socket.io/` |
| `WithHeaders(h)` | Headers HTTP no handshake (clonados na construção) |
| `On(event, handler)` | Regista handler para um evento Socket.IO |
| `OnConnect(fn)` | Callback quando a sessão fica pronta |
| `OnDisconnect(fn)` | Callback quando o transporte cai |
| `Connect()` | Inicia conexão e reconnect automático |
| `Emit(event, data...)` | Envia JSON; retorna `ErrNotConnected` se offline |
| `Close()` | Para reconnect e fecha a conexão |

### Erros exportados

| Erro | Quando |
|---|---|
| `ErrEmptyAddr` | `NewClient("")` |
| `ErrNotConnected` | `Emit` com transporte offline |
| `ErrAlreadyConnected` | `Connect()` chamado duas vezes |

## Namespaces

Apenas o namespace default é suportado. Eventos recebidos noutros namespaces (`/admin`, etc.) são descartados com log de aviso.

## Arquitetura interna

```
App → Client → engineio → WebSocket
         ↓
      parser
```

- **`Client`** — API pública e loops (`readLoop`, `writeLoop`, reconnect).
- **`engineio/`** — handshake Engine.IO, heartbeat, dial WebSocket.
- **`parser/`** — encode/decode do protocolo Socket.IO.

## Documentação adicional

- [PLANO_EVOLUCAO_CLIENTE.md](./PLANO_EVOLUCAO_CLIENTE.md) — objetivos, decisões de design e roadmap técnico
- [AUDITORIA_CLIENTE.md](./AUDITORIA_CLIENTE.md) — auditoria do código original (pré-refactor) e motivação do fork

## Soak test (opcional)

Ferramentas em `cmd/` para validar reconnect e estabilidade de goroutines/memória contra um servidor Socket.IO real. **Não fazem parte da biblioteca** — servem apenas para testes locais.

### `cmd/testclient`

Client de soak que emite eventos periodicamente e imprime métricas de runtime.

```bash
# Na raiz do repositório
go run ./cmd/testclient
go run ./cmd/testclient -url http://localhost:8083 -token 123
```

| Variável / flag | Descrição | Default |
|---|---|---|
| `-url` / `SOCKET_URL` | URL do servidor | `http://localhost:8083` |
| `-token` / `SOCKET_TOKEN` | Token Bearer no handshake | `123` |

**Logs de evento:** `[CONNECT]`, `[DISCONNECT]`, `[EMIT]`, `[RECEIVE]`, `[RECONNECT]`, `[REPORT]`.

O `[REPORT]` periódico inclui uptime, reconnects, mensagens, memória e goroutines.

Intervalos (`emitInterval`, `reportInterval`) ficam no topo de `cmd/testclient/main.go`.

### `cmd/server-churn.sh`

Sobe e derruba o servidor em ciclos fixos — útil para simular instabilidade de rede/host.

**Fase 1:** servidor up por `CHURN_INTERVAL` segundos, depois down — repete por `CHURN_DURATION` segundos.  
**Fase 2:** servidor estável por `STABLE_DURATION` segundos.

```bash
export SERVER_DIR=/caminho/para/seu-servidor-socket-io
./cmd/server-churn.sh
```

| Variável | Descrição | Default |
|---|---|---|
| `SERVER_DIR` | Diretório do projeto do servidor (obrigatório) | — |
| `SERVER_URL` | URL base do servidor | `http://localhost:8083` |
| `PING_URL` | Health check HTTP | `{SERVER_URL}/v2/ping` |
| `SOAK_SERVER_LOG` | Log do script e stdout do servidor | `$TMPDIR/go-socket-io-server-churn.log` |
| `CHURN_INTERVAL` | Segundos com servidor up por ciclo | `30` |
| `CHURN_DURATION` | Duração total da fase 1 (segundos) | `600` (10 min) |
| `STABLE_DURATION` | Duração da fase 2 (segundos) | `1800` (30 min) |

Requisitos: `npm run start` funcional no `SERVER_DIR`, `curl` e `lsof`.

### `cmd/soak-until-100.sh`

Orquestra `testclient` + churn do servidor até atingir `TARGET_RECONNECTS`. Para tudo e imprime os 3 primeiros e 3 últimos `[REPORT]` completos.

```bash
export SERVER_DIR=/caminho/para/seu-servidor-socket-io
./cmd/soak-until-100.sh
```

| Variável | Descrição | Default |
|---|---|---|
| `SERVER_DIR` | Diretório do projeto do servidor (obrigatório) | — |
| `SERVER_URL` | URL passada ao testclient | `http://localhost:8083` |
| `TARGET_RECONNECTS` | Reconnects alvo para encerrar | `100` |
| `CHURN_INTERVAL` | Segundos com servidor up por ciclo | `30` |
| `SOAK_CLIENT_LOG` | Log do testclient | `$TMPDIR/go-socket-io-testclient.log` |
| `SOAK_SERVER_LOG` | Log do servidor | `$TMPDIR/go-socket-io-server-churn.log` |
| `SOAK_REPORTS_FILE` | Reports extraídos no final | `$TMPDIR/go-socket-io-reports.txt` |

**Fluxo típico:**

1. Terminal A — `./cmd/soak-until-100.sh` (client + server automatizados)
2. Ou manual — Terminal A: `./cmd/server-churn.sh` · Terminal B: `go run ./cmd/testclient`

## Badges

![Build Status](https://github.com/Joaquimborges/go-socket.io/workflows/CI/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/Joaquimborges/go-socket.io)](https://goreportcard.com/report/github.com/Joaquimborges/go-socket.io)

## Licença

BSD 3-Clause — ver [LICENSE](./LICENSE).
