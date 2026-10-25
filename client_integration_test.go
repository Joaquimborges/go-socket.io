package socketio

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

const waitTimeout = 5 * time.Second

// fakeServer speaks just enough Engine.IO v4 over WebSocket and Socket.IO v5 to drive the client: it opens the
// session, answers every CONNECT through accept, and hands each connection to the test.
type fakeServer struct {
	srv    *httptest.Server
	conns  chan *fakeConn
	accept func(attempt int) string

	attempts atomic.Int32
}

// fakeConn is one client connection as the server sees it. packets carries every Socket.IO packet the client
// sent after CONNECT, without the Engine.IO message prefix.
type fakeConn struct {
	ws       *websocket.Conn
	writeMu  sync.Mutex
	connects chan string
	packets  chan string
}

// newFakeServer accepts every CONNECT unless accept says otherwise. accept returns the Socket.IO packet that
// answers attempt n (starting at 1), or "" to never answer.
func newFakeServer(t *testing.T, accept func(attempt int) string) *fakeServer {
	t.Helper()

	if accept == nil {
		accept = func(int) string { return `0{"sid":"socket-sid"}` }
	}

	s := &fakeServer{
		conns:  make(chan *fakeConn, 16),
		accept: accept,
	}

	upgrader := websocket.Upgrader{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		conn := &fakeConn{
			ws:       ws,
			connects: make(chan string, 4),
			packets:  make(chan string, 64),
		}

		_ = conn.write(`0{"sid":"engine-sid","upgrades":[],"pingInterval":25000,"pingTimeout":20000}`)
		s.conns <- conn
		go s.serve(conn)
	}))

	t.Cleanup(s.srv.Close)

	return s
}

func (s *fakeServer) serve(conn *fakeConn) {
	defer close(conn.packets)

	for {
		_, msg, err := conn.ws.ReadMessage()
		if err != nil {
			return
		}

		text := string(msg)
		if !strings.HasPrefix(text, "4") {
			continue // Engine.IO PONG and the like
		}

		packet := text[1:]
		if strings.HasPrefix(packet, "0") {
			conn.connects <- packet
			if reply := s.accept(int(s.attempts.Add(1))); reply != "" {
				_ = conn.send(reply)
			}

			continue
		}

		conn.packets <- packet
	}
}

func (s *fakeServer) url() string {
	return s.srv.URL
}

func (s *fakeServer) nextConn(t *testing.T) *fakeConn {
	t.Helper()

	select {
	case conn := <-s.conns:
		return conn
	case <-time.After(waitTimeout):
		t.Fatal("no connection reached the server")

		return nil
	}
}

func (c *fakeConn) write(text string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	return c.ws.WriteMessage(websocket.TextMessage, []byte(text))
}

// send writes a Socket.IO packet inside an Engine.IO message.
func (c *fakeConn) send(packet string) error {
	return c.write("4" + packet)
}

func (c *fakeConn) nextPacket(t *testing.T) string {
	t.Helper()

	select {
	case packet, ok := <-c.packets:
		if !ok {
			t.Fatal("connection closed before the expected packet")
		}

		return packet
	case <-time.After(waitTimeout):
		t.Fatal("no packet from the client")

		return ""
	}
}

func (c *fakeConn) waitConnect(t *testing.T) {
	t.Helper()

	select {
	case <-c.connects:
	case <-time.After(waitTimeout):
		t.Fatal("client never sent CONNECT")
	}
}

// waitClosed waits for the connection to end, and fails if the client sends anything on it first.
func (c *fakeConn) waitClosed(t *testing.T) {
	t.Helper()

	select {
	case packet, ok := <-c.packets:
		if ok {
			t.Fatalf("unexpected packet %q before the connection closed", packet)
		}
	case <-time.After(waitTimeout):
		t.Fatal("connection was not closed")
	}
}

// newConnectedClient starts a client against server. setup registers handlers and callbacks, which must happen
// before Connect.
func newConnectedClient(t *testing.T, server *fakeServer, setup func(*Client)) (*Client, chan struct{}) {
	t.Helper()

	client, err := NewClient(server.url())
	require.NoError(t, err)

	connected := make(chan struct{}, 4)
	client.OnConnect(func() { connected <- struct{}{} })
	setup(client)

	require.NoError(t, client.Connect())
	t.Cleanup(func() { _ = client.Close() })

	return client, connected
}

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(waitTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestClientConnectEmitAndReceive(t *testing.T) {
	server := newFakeServer(t, nil)

	received := make(chan string, 1)
	client, connected := newConnectedClient(t, server, func(c *Client) {
		c.On("greet", func(msg string) { received <- msg })
	})

	conn := server.nextConn(t)
	waitSignal(t, connected, "OnConnect")

	require.NoError(t, client.Emit("hello", map[string]int{"a": 1}))
	require.Equal(t, "2[\"hello\",{\"a\":1}]\n", conn.nextPacket(t))

	require.NoError(t, conn.send(`2["greet","hi"]`))
	select {
	case msg := <-received:
		require.Equal(t, "hi", msg)
	case <-time.After(waitTimeout):
		t.Fatal("handler not called")
	}
}

func TestClientAcksServerEventWithEmptyReceipt(t *testing.T) {
	server := newFakeServer(t, nil)

	_, connected := newConnectedClient(t, server, func(c *Client) {
		c.On("greet", func(string) {})
	})

	conn := server.nextConn(t)
	waitSignal(t, connected, "OnConnect")

	// The socket server relies on exactly this empty receipt, for handled and unhandled events alike.
	require.NoError(t, conn.send(`25["greet","hi"]`))
	require.Equal(t, "35", conn.nextPacket(t))

	require.NoError(t, conn.send(`26["nobody_listens"]`))
	require.Equal(t, "36", conn.nextPacket(t))
}

func TestClientConnectErrorIsReportedAndRetried(t *testing.T) {
	server := newFakeServer(t, func(attempt int) string {
		if attempt == 1 {
			return `4{"message":"invalid service","data":{"code":1}}`
		}

		return `0{"sid":"socket-sid"}`
	})

	client, err := NewClient(server.url())
	require.NoError(t, err)

	attemptErrs := make(chan error, 4)
	client.OnReconnectAttempt(func(_ int, _ time.Duration, err error) { attemptErrs <- err })

	connected := make(chan struct{}, 4)
	client.OnConnect(func() { connected <- struct{}{} })

	require.NoError(t, client.Connect())
	t.Cleanup(func() { _ = client.Close() })

	first := server.nextConn(t)

	select {
	case err := <-attemptErrs:
		var connectErr *ConnectError
		require.ErrorAs(t, err, &connectErr)
		require.Equal(t, "invalid service", connectErr.Message)
		require.JSONEq(t, `{"code":1}`, string(connectErr.Data))
	case <-time.After(waitTimeout):
		t.Fatal("connect error not reported")
	}

	// A refused connection is not a connected one, and its transport is dropped.
	select {
	case <-connected:
		t.Fatal("OnConnect called for a refused connection")
	default:
	}
	require.ErrorIs(t, client.Emit("hello"), ErrNotConnected)
	first.waitClosed(t)

	second := server.nextConn(t)
	waitSignal(t, connected, "OnConnect after retry")

	require.NoError(t, client.Emit("hello"))
	require.Equal(t, "2[\"hello\"]\n", second.nextPacket(t))
}

func TestClientDropsAckOfEndedSession(t *testing.T) {
	server := newFakeServer(t, nil)

	inHandler := make(chan struct{})
	release := make(chan struct{})
	handlerDone := make(chan struct{})

	client, connected := newConnectedClient(t, server, func(c *Client) {
		c.On("slow", func(string) {
			close(inHandler)
			<-release
			close(handlerDone)
		})
	})

	first := server.nextConn(t)
	waitSignal(t, connected, "OnConnect")

	require.NoError(t, first.send(`27["slow","x"]`))
	waitSignal(t, inHandler, "handler start")

	// What the write loop does on a write error, while the read loop is still inside the handler.
	session := client.currentSession()
	require.NotNil(t, session)
	session.end(errors.New("injected write failure"))

	second := server.nextConn(t)
	waitSignal(t, connected, "OnConnect after reconnect")

	close(release)
	waitSignal(t, handlerDone, "handler end")

	// The ack for id 7 belongs to the first connection; the second must see only what is emitted on it.
	require.NoError(t, client.Emit("after"))
	require.Equal(t, "2[\"after\"]\n", second.nextPacket(t))

	// And the stale read loop is gone: the second connection still answers its own events, once.
	require.NoError(t, second.send(`28["nobody_listens"]`))
	require.Equal(t, "38", second.nextPacket(t))
}

func TestClientReconnectsAfterServerClosesTransport(t *testing.T) {
	server := newFakeServer(t, nil)

	disconnected := make(chan struct{}, 4)
	client, connected := newConnectedClient(t, server, func(c *Client) {
		c.OnDisconnect(func(error) { disconnected <- struct{}{} })
	})

	first := server.nextConn(t)
	waitSignal(t, connected, "OnConnect")

	require.NoError(t, first.ws.Close())
	waitSignal(t, disconnected, "OnDisconnect")

	second := server.nextConn(t)
	waitSignal(t, connected, "OnConnect after reconnect")

	require.NoError(t, client.Emit("back"))
	require.Equal(t, "2[\"back\"]\n", second.nextPacket(t))
}

func TestClientCloseDuringHandshake(t *testing.T) {
	server := newFakeServer(t, func(int) string { return "" })

	client, err := NewClient(server.url())
	require.NoError(t, err)

	var attempts atomic.Int32
	client.OnReconnectAttempt(func(int, time.Duration, error) { attempts.Add(1) })
	client.OnConnect(func() { t.Error("OnConnect called without the server accepting") })

	require.NoError(t, client.Connect())

	conn := server.nextConn(t)
	conn.waitConnect(t)

	require.NoError(t, client.Close())
	conn.waitClosed(t)

	// Closing ends the loop: no retry is reported and no new connection is made.
	select {
	case <-server.conns:
		t.Fatal("client reconnected after Close")
	case <-time.After(initialBackoff + 500*time.Millisecond):
	}
	require.Zero(t, attempts.Load())
}
