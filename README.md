# go-socket.io

Cliente [Socket.IO](https://socket.io) v4 em Go — pensado para backends que precisam de uma conexão persistente, estável e de longa duração com um servidor Socket.IO (Node.js).

Fork privado de [googollee/go-socket.io](https://github.com/googollee/go-socket.io), simplificado para **cliente only**: sem servidor, sem rooms, sem broadcast, sem ACK.

## Status

A biblioteca está em evolução ativa. A API abaixo é o alvo; partes do código legado (servidor, namespaces, ACK) ainda serão removidas nos próximos PRs.

| Suportado (alvo) | Fora de escopo |
|---|---|
| Conexão WebSocket + JSON | Servidor Socket.IO |
| Namespace root (`/` ou `""`) | Rooms e broadcast |
| `Emit` / `On` de eventos | ACK / callbacks de resposta |
| Reconnect automático (backoff) | Namespaces customizados |
| Heartbeat PING → PONG | Payloads binários na API pública |

**Requisitos:** Go 1.26+, servidor Socket.IO **v4**, transporte WebSocket.

## Instalação

```bash
go get github.com/Joaquimborges/go-socket.io
```

```go
import socketio "github.com/Joaquimborges/go-socket.io"
```

## Uso

```go
package main

import (
    "errors"
    "log"

    socketio "github.com/Joaquimborges/go-socket.io"
)

func main() {
    client := socketio.NewClient("https://api.example.com")

    client.On("machine_connected", func(data MachineConnected) {
        log.Println("machine connected:", data)
    })

    client.OnConnect(func() {
        log.Println("connected") // inclui reconexões
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

    // ... manter o processo vivo ...

    _ = client.Close()
}
```

### API

| Método | Descrição |
|---|---|
| `NewClient(url)` | Cria o cliente apontando para o servidor Socket.IO |
| `On(event, handler)` | Registra handler síncrono para um evento |
| `OnConnect(fn)` | Chamado quando a conexão é estabelecida (1ª vez e após cada reconnect) |
| `OnDisconnect(fn)` | Chamado quando o transporte cai, antes do backoff |
| `Connect()` | Inicia os loops de leitura/escrita e o reconnect |
| `Emit(event, data...)` | Envia JSON; retorna `error` (`ErrNotConnected` se offline) |
| `Close()` | Para o reconnect e fecha a conexão |

## Arquitetura

```
App → Client → engineio → WebSocket
         ↓
      parser
```

- **`Client`** — único ponto de entrada da API pública.
- **`engineio/`** — camada Engine.IO (handshake, heartbeat, transporte).
- **`parser/`** — codificação/decodificação do protocolo Socket.IO.

Dois loops internos (`readLoop` + `writeLoop`) mantêm a conexão; erros fatais saem do `readLoop` e disparam reconnect com backoff (1s → 2s → … → máx. 30s).

## Namespaces

Apenas o namespace default é suportado. Eventos recebidos em outros namespaces (`/admin`, etc.) são descartados com log de aviso — o cliente não processa eventos fora do root por acidente.

## Badges

![Build Status](https://github.com/Joaquimborges/go-socket.io/workflows/CI/badge.svg)
[![Go Report Card](https://goreportcard.com/badge/github.com/Joaquimborges/go-socket.io)](https://goreportcard.com/report/github.com/Joaquimborges/go-socket.io)

## Licença

BSD 3-Clause — ver [LICENSE](./LICENSE).
