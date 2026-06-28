package socketio

import (
	"errors"
	"log"
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
)

// Client is a Socket.IO client for the default namespace.
type Client struct {
	url string

	engineConn engineio.Conn
	encoder    *parser.Encoder
	decoder    *parser.Decoder

	writeChan chan parser.Payload
	quitChan  chan struct{}
	closeOnce sync.Once

	connectMu    sync.Mutex
	loopsStarted bool

	closed atomic.Bool

	mu        sync.RWMutex
	connected bool

	events     map[string]*eventHandler
	eventsLock sync.RWMutex

	onConnect    func()
	onDisconnect func(err error)
}

// NewClient creates a client for the given Socket.IO server URL.
func NewClient(addr string) (*Client, error) {
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

	return &Client{
		url:    u.String(),
		events: make(map[string]*eventHandler),
	}, nil
}

// Connect starts the client loops and automatic reconnect with backoff.
func (c *Client) Connect() error {
	c.connectMu.Lock()
	defer c.connectMu.Unlock()

	if c.loopsStarted {
		return ErrAlreadyConnected
	}

	c.writeChan = make(chan parser.Payload)
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

// Emit sends an event with JSON-encoded arguments.
func (c *Client) Emit(event string, args ...interface{}) error {
	if !c.isConnected() {
		return ErrNotConnected
	}

	data := make([]interface{}, len(args)+1)
	data[0] = event
	copy(data[1:], args)

	pkg := parser.Payload{
		Header: parser.Header{Type: parser.Event},
		Data:   data,
	}

	select {
	case c.writeChan <- pkg:
		return nil
	case <-c.quitChan:
		return ErrNotConnected
	}
}

func (c *Client) run() {
	backoff := initialBackoff

	for {
		if c.closed.Load() {
			return
		}

		if err := c.connectOnce(); err != nil {
			log.Printf("socketio: connect failed: %v", err)

			if !c.waitBackoff(&backoff) {
				return
			}

			continue
		}

		backoff = initialBackoff

		sessionEnd := make(chan struct{})
		var sessionErr error
		var sessionOnce sync.Once

		endSession := func(err error) {
			sessionOnce.Do(func() {
				sessionErr = err
				close(sessionEnd)
			})
		}

		go c.readLoop(endSession)
		go c.writeLoop(endSession)

		<-sessionEnd
		err := sessionErr
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

			go fn(err)
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

func (c *Client) connectOnce() error {
	_ = c.closeTransport()

	dialer := engineio.Dialer{
		Transports: []transport.Transport{websocket.Default},
	}

	engineConn, err := dialer.Dial(c.url, nil)
	if err != nil {
		return err
	}

	c.engineConn = engineConn
	c.encoder = parser.NewEncoder(engineConn)
	c.decoder = parser.NewDecoder(engineConn)

	header := parser.Header{Type: parser.Connect}

	return c.encoder.Encode(header)
}

func (c *Client) closeTransport() error {
	c.setConnected(false)

	if c.engineConn == nil {
		return nil
	}

	err := c.engineConn.Close()
	c.engineConn = nil
	c.encoder = nil
	c.decoder = nil

	return err
}

func (c *Client) readLoop(endSession func(error)) {
	var err error

	defer func() {
		endSession(err)
	}()

	for {
		if c.closed.Load() {
			return
		}

		var header parser.Header
		var event string

		if err = c.decoder.DecodeHeader(&header, &event); err != nil {
			return
		}

		if !c.validateNamespace(header.Namespace) {
			continue
		}

		switch header.Type {
		case parser.Ack:
			err = c.decoder.DiscardLast()
		case parser.Connect:
			err = c.handleConnect()
		case parser.Disconnect:
			err = c.handleDisconnect()
		case parser.Event:
			err = c.handleEvent(event, header)
		}

		if err != nil {
			if !errors.Is(err, errServerDisconnect) {
				log.Printf("socketio: read error: %v", err)
			}

			return
		}
	}
}

func (c *Client) writeLoop(endSession func(error)) {
	for {
		select {
		case <-c.quitChan:
			return
		case pkg := <-c.writeChan:
			var err error
			if len(pkg.Data) > 0 {
				err = c.encoder.Encode(pkg.Header, pkg.Data)
			} else {
				err = c.encoder.Encode(pkg.Header)
			}

			if err != nil {
				log.Printf("socketio: write error: %v", err)
				endSession(err)

				return
			}
		}
	}
}

func (c *Client) validateNamespace(namespace string) bool {
	switch namespace {
	case "", aliasRootNamespace:
		return true
	default:
		log.Printf("socketio: unsupported namespace %q, discarding packet", namespace)
		_ = c.decoder.DiscardLast()

		return false
	}
}

func (c *Client) handleConnect() error {
	if err := c.decoder.DiscardLast(); err != nil {
		return err
	}

	c.setConnected(true)

	if fn := c.onConnect; fn != nil {
		go fn()
	}

	return nil
}

func (c *Client) handleDisconnect() error {
	_, err := c.decoder.DecodeArgs([]reflect.Type{reflect.TypeOf("")})
	if err != nil {
		return err
	}

	c.setConnected(false)

	return errServerDisconnect
}

func (c *Client) handleEvent(event string, header parser.Header) error {
	handler, ok := c.getEventHandler(event)
	if !ok {
		if err := c.decoder.DiscardLast(); err != nil {
			return err
		}

		if header.NeedAck {
			return c.sendAck(header)
		}

		return nil
	}

	args, err := c.decoder.DecodeArgs(handler.argTypes)
	if err != nil {
		return err
	}

	if err := handler.Call(args); err != nil {
		return err
	}

	if header.NeedAck {
		return c.sendAck(header)
	}

	return nil
}

func (c *Client) sendAck(header parser.Header) error {
	ack := parser.Payload{
		Header: parser.Header{
			Type:    parser.Ack,
			ID:      header.ID,
			NeedAck: true,
		},
	}

	select {
	case c.writeChan <- ack:
		return nil
	case <-c.quitChan:
		return ErrNotConnected
	}
}

func (c *Client) getEventHandler(event string) (*eventHandler, bool) {
	c.eventsLock.RLock()
	defer c.eventsLock.RUnlock()

	handler, ok := c.events[event]

	return handler, ok
}

func (c *Client) setConnected(connected bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.connected = connected
}

func (c *Client) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.connected
}

const (
	aliasRootNamespace = "/"
	rootNamespace      = ""
)

var (
	errServerDisconnect = errors.New("socketio: server disconnect")
	errTransportLost    = errors.New("socketio: transport connection lost")
)
