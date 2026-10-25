package socketio

import (
	"errors"
	"log"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Joaquimborges/go-socket.io/engineio"
	"github.com/Joaquimborges/go-socket.io/engineio/transport"
	"github.com/Joaquimborges/go-socket.io/engineio/transport/websocket"
	"github.com/Joaquimborges/go-socket.io/parser"
)

const (
	maxReconnectBackoff = 30 * time.Second
	initialBackoff      = time.Second

	// connectTimeout bounds the wait for the server to accept the Socket.IO connection, as the JS client's
	// default timeout does.
	connectTimeout = 20 * time.Second
)

// Client is a Socket.IO client for the default namespace.
type Client struct {
	url     string
	headers http.Header

	quitChan  chan struct{}
	closeOnce sync.Once

	connectMu    sync.Mutex
	loopsStarted bool

	closed atomic.Bool

	// mu guards the transport being dialed or in use and the session accepted on it. current is nil until the
	// server accepts the connection and again once it ends, which is what Emit reads as "not connected".
	mu         sync.Mutex
	engineConn engineio.Conn
	current    *session

	events     map[string]*eventHandler
	eventsLock sync.RWMutex

	onConnect          func()
	onDisconnect       func(err error)
	onReconnectAttempt func(attempt int, backoff time.Duration, err error)
}

// session is one accepted Socket.IO connection. Everything its loops read and write travels with it, so a loop
// left over from an earlier connection can never read from or write to the next one.
type session struct {
	encoder   *parser.Encoder
	decoder   *parser.Decoder
	writeChan chan parser.Payload

	done    chan struct{}
	endOnce sync.Once
	err     error
}

func newSession(encoder *parser.Encoder, decoder *parser.Decoder) *session {
	return &session{
		encoder:   encoder,
		decoder:   decoder,
		writeChan: make(chan parser.Payload),
		done:      make(chan struct{}),
	}
}

// end records the first reason the session ended and releases everything waiting on it.
func (s *session) end(err error) {
	s.endOnce.Do(func() {
		s.err = err
		close(s.done)
	})
}

// NewClient creates a client for the given Socket.IO server URL.
func NewClient(addr string, opts ...ClientOption) (*Client, error) {
	if addr == "" {
		return nil, ErrEmptyAddr
	}

	u, err := url.Parse(addr)
	if err != nil {
		return nil, err
	}

	u.Path = path.Join("/socket.io", rootNamespace)
	u.Path = u.EscapedPath()

	if strings.HasSuffix(u.Path, "socket.io") {
		u.Path += "/"
	}

	client := &Client{
		url:    u.String(),
		events: make(map[string]*eventHandler),
	}

	for _, opt := range opts {
		opt(client)
	}

	return client, nil
}

// Connect starts the client loops and automatic reconnect with backoff.
func (c *Client) Connect() error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()

	if c.loopsStarted {
		return ErrAlreadyConnected
	}

	c.quitChan = make(chan struct{})
	c.loopsStarted = true

	go c.run()

	return nil
}

// Close stops reconnect and closes the transport.
func (c *Client) Close() error {
	var err error

	c.closeOnce.Do(func() {
		c.closed.Store(true)

		if c.quitChan != nil {
			close(c.quitChan)
		}

		err = c.closeTransport()
	})

	return err
}

// On registers a synchronous handler for a Socket.IO event.
func (c *Client) On(event string, f interface{}) {
	c.eventsLock.Lock()
	defer c.eventsLock.Unlock()

	c.events[event] = newEventHandler(f)
}

// OnConnect registers a callback invoked when the Socket.IO session is established.
func (c *Client) OnConnect(f func()) {
	c.onConnect = f
}

// OnDisconnect registers a callback invoked when the transport fails or closes.
func (c *Client) OnDisconnect(f func(err error)) {
	c.onDisconnect = f
}

// OnReconnectAttempt registers a callback invoked when a connection attempt fails before backoff. A server that
// refuses the connection reports a *ConnectError here.
func (c *Client) OnReconnectAttempt(f func(attempt int, backoff time.Duration, err error)) {
	c.onReconnectAttempt = f
}

// Emit sends an event with JSON-encoded arguments.
func (c *Client) Emit(event string, args ...interface{}) error {
	s := c.currentSession()
	if s == nil {
		return ErrNotConnected
	}

	data := make([]interface{}, len(args)+1)
	data[0] = event
	copy(data[1:], args)

	pkg := parser.Payload{
		Header: parser.Header{Type: parser.Event},
		Data:   data,
	}

	return c.send(s, pkg)
}

// send hands a packet to the session's write loop, and fails once that session or the client is gone.
func (c *Client) send(s *session, pkg parser.Payload) error {
	select {
	case s.writeChan <- pkg:
		return nil
	case <-s.done:
		return ErrNotConnected
	case <-c.quitChan:
		return ErrNotConnected
	}
}

func (c *Client) run() {
	backoff := initialBackoff
	reconnectAttempt := 0

	for {
		if c.closed.Load() {
			return
		}

		s, err := c.connectOnce()
		if err != nil {
			if c.closed.Load() {
				return
			}

			reconnectAttempt++
			if fn := c.onReconnectAttempt; fn != nil {
				fn(reconnectAttempt, backoff, err)
			}

			if !c.waitBackoff(&backoff) {
				return
			}

			continue
		}

		reconnectAttempt = 0
		backoff = initialBackoff

		if fn := c.onConnect; fn != nil {
			go fn()
		}

		go c.readLoop(s)
		go c.writeLoop(s)

		<-s.done
		err = s.err
		if closeErr := c.closeTransport(); closeErr != nil && err == nil {
			err = closeErr
		}

		if c.closed.Load() {
			return
		}

		if fn := c.onDisconnect; fn != nil {
			if err == nil {
				err = errTransportLost
			}

			go func(disconnectErr error) {
				fn(disconnectErr)
			}(err)
		}

		if !c.waitBackoff(&backoff) {
			return
		}
	}
}

func (c *Client) waitBackoff(backoff *time.Duration) bool {
	timer := time.NewTimer(*backoff)
	defer timer.Stop()

	select {
	case <-c.quitChan:
		return false
	case <-timer.C:
	}

	*backoff *= 2
	if *backoff > maxReconnectBackoff {
		*backoff = maxReconnectBackoff
	}

	return true
}

// connectOnce dials the transport and waits for the server to accept the Socket.IO connection. Only an accepted
// connection counts as connected, so a refusal is a failed attempt and keeps the backoff growing.
func (c *Client) connectOnce() (*session, error) {
	dialer := engineio.Dialer{
		Transports: []transport.Transport{websocket.Default},
	}

	engineConn, err := dialer.Dial(c.url, c.headers)
	if err != nil {
		return nil, err
	}

	// Close may run while the dial is in flight: either it sees this transport and closes it, or the check
	// below does.
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		_ = engineConn.Close()

		return nil, errClientClosed
	}
	c.engineConn = engineConn
	c.mu.Unlock()

	s, err := handshake(engineConn)
	if err != nil {
		_ = c.closeTransport()

		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.engineConn != engineConn {
		return nil, errClientClosed
	}
	c.current = s

	return s, nil
}

// handshake sends CONNECT on the root namespace and reads until the server accepts or refuses it.
func handshake(engineConn engineio.Conn) (*session, error) {
	encoder := parser.NewEncoder(engineConn)
	decoder := parser.NewDecoder(engineConn)

	if err := encoder.Encode(parser.Header{Type: parser.Connect}); err != nil {
		return nil, err
	}

	var timedOut atomic.Bool
	timer := time.AfterFunc(connectTimeout, func() {
		timedOut.Store(true)
		_ = engineConn.Close()
	})

	for {
		var header parser.Header
		var event string

		if err := decoder.DecodeHeader(&header, &event); err != nil {
			timer.Stop()
			if timedOut.Load() {
				return nil, errConnectTimeout
			}

			return nil, err
		}

		if !validateNamespace(decoder, header.Namespace) {
			continue
		}

		switch header.Type {
		case parser.Connect:
			err := decoder.DiscardLast()
			if !timer.Stop() {
				// The timeout fired as the reply arrived and has already closed the transport.
				return nil, errConnectTimeout
			}
			if err != nil {
				return nil, err
			}

			return newSession(encoder, decoder), nil

		case parser.Error:
			timer.Stop()
			raw, err := decoder.DecodeRaw()
			if err != nil {
				return nil, err
			}

			return nil, newConnectError(raw)

		default:
			// Nothing else is meaningful before the server accepts the connection.
			if err := decoder.DiscardLast(); err != nil {
				timer.Stop()

				return nil, err
			}
		}
	}
}

func (c *Client) closeTransport() error {
	c.mu.Lock()
	engineConn := c.engineConn
	c.engineConn = nil
	c.current = nil
	c.mu.Unlock()

	if engineConn == nil {
		return nil
	}

	return engineConn.Close()
}

func (c *Client) readLoop(s *session) {
	var err error

	defer func() {
		s.end(err)
	}()

	for {
		select {
		case <-s.done:
			return
		default:
		}

		if c.closed.Load() {
			return
		}

		var header parser.Header
		var event string

		if err = s.decoder.DecodeHeader(&header, &event); err != nil {
			return
		}

		if !validateNamespace(s.decoder, header.Namespace) {
			continue
		}

		switch header.Type {
		case parser.Disconnect:
			err = c.handleDisconnect(s)
		case parser.Event:
			err = c.handleEvent(s, event, header)
		default:
			// Server ACKs, a repeated CONNECT and errors on an accepted connection carry nothing to act on.
			err = s.decoder.DiscardLast()
		}

		if err != nil {
			if !errors.Is(err, errServerDisconnect) && !errors.Is(err, ErrNotConnected) {
				log.Printf("socketio: read error: %v", err)
			}

			return
		}
	}
}

func (c *Client) writeLoop(s *session) {
	for {
		select {
		case <-c.quitChan:
			return
		case <-s.done:
			return
		case pkg := <-s.writeChan:
			var err error
			if len(pkg.Data) > 0 {
				err = s.encoder.Encode(pkg.Header, pkg.Data)
			} else {
				err = s.encoder.Encode(pkg.Header)
			}

			if err != nil {
				log.Printf("socketio: write error: %v", err)
				s.end(err)

				return
			}
		}
	}
}

func validateNamespace(decoder *parser.Decoder, namespace string) bool {
	switch namespace {
	case "", aliasRootNamespace:
		return true
	default:
		log.Printf("socketio: unsupported namespace %q, discarding packet", namespace)
		_ = decoder.DiscardLast()

		return false
	}
}

func (c *Client) handleDisconnect(s *session) error {
	_, err := s.decoder.DecodeArgs([]reflect.Type{reflect.TypeOf("")})
	if err != nil {
		return err
	}

	return errServerDisconnect
}

func (c *Client) handleEvent(s *session, event string, header parser.Header) error {
	handler, ok := c.getEventHandler(event)
	if !ok {
		if err := s.decoder.DiscardLast(); err != nil {
			return err
		}

		if header.NeedAck {
			return c.sendAck(s, header)
		}

		return nil
	}

	args, err := s.decoder.DecodeArgs(handler.argTypes)
	if err != nil {
		return err
	}

	if err := handler.Call(args); err != nil {
		return err
	}

	if header.NeedAck {
		return c.sendAck(s, header)
	}

	return nil
}

// sendAck answers on the session the event arrived on. An ack id means nothing on any other connection, so once
// that session has ended the ack is dropped instead of waiting for the next one.
func (c *Client) sendAck(s *session, header parser.Header) error {
	ack := parser.Payload{
		Header: parser.Header{
			Type:    parser.Ack,
			ID:      header.ID,
			NeedAck: true,
		},
	}

	return c.send(s, ack)
}

func (c *Client) getEventHandler(event string) (*eventHandler, bool) {
	c.eventsLock.RLock()
	defer c.eventsLock.RUnlock()

	handler, ok := c.events[event]

	return handler, ok
}

func (c *Client) currentSession() *session {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.current
}

const (
	aliasRootNamespace = "/"
	rootNamespace      = ""
)

var (
	errServerDisconnect = errors.New("socketio: server disconnect")
	errTransportLost    = errors.New("socketio: transport connection lost")
	errConnectTimeout   = errors.New("socketio: timed out waiting for the server to accept the connection")
	errClientClosed     = errors.New("socketio: client closed")
)
