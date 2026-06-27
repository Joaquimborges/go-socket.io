# engineio

Implementação Go da camada [Engine.IO](https://github.com/socketio/engine.io), usada internamente pelo cliente Socket.IO deste repositório.

Suporta transporte **polling** e **WebSocket** no lado cliente (`Dialer`). O código de servidor (`Server`, session manager) foi removido — este fork é **cliente only**.

## Uso (cliente)

```go
import (
    "github.com/Joaquimborges/go-socket.io/engineio"
    "github.com/Joaquimborges/go-socket.io/engineio/transport"
    "github.com/Joaquimborges/go-socket.io/engineio/transport/websocket"
)

dialer := engineio.Dialer{
    Transports: []transport.Transport{websocket.Default},
}

conn, err := dialer.Dial("https://example.com/socket.io/", nil)
```

Para a API pública de alto nível, use o pacote raiz `github.com/Joaquimborges/go-socket.io`.

## Licença

BSD 3-Clause — ver [LICENSE](../LICENSE).
