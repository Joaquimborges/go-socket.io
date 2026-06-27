package transport

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/Joaquimborges/go-socket.io/engineio/frame"
	"github.com/Joaquimborges/go-socket.io/engineio/packet"
)

// Transport creates transport connections for dial (client) or accept (server).
type Transport interface {
	Name() string
	Accept(w http.ResponseWriter, r *http.Request) (Conn, error)
	Dial(u *url.URL, requestHeader http.Header) (Conn, error)
}

// FrameReader reads a frame. It needs be closed before next reading.
type FrameReader interface {
	NextReader() (frame.Type, packet.Type, io.ReadCloser, error)
}

// FrameWriter writes a frame. It needs be closed before next writing.
type FrameWriter interface {
	NextWriter(ft frame.Type, pt packet.Type) (io.WriteCloser, error)
}

// Conn is a transport connection.
type Conn interface {
	FrameReader
	FrameWriter
	io.Closer
	URL() url.URL
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	RemoteHeader() http.Header
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error
}
