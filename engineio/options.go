package engineio

import (
	"time"

	"github.com/Joaquimborges/go-socket.io/engineio/transport"
)

// Options configures engine.io client connections.
type Options struct {
	PingTimeout  time.Duration
	PingInterval time.Duration
	Transports   []transport.Transport
}
