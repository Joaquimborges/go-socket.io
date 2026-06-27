package socketio

import (
	"errors"
	"log"
	"net/url"
	"path"
	"reflect"
	"strings"
	"sync"

	"github.com/Joaquimborges/go-socket.io/engineio"
	"github.com/Joaquimborges/go-socket.io/engineio/transport"
	"github.com/Joaquimborges/go-socket.io/engineio/transport/polling"
	"github.com/Joaquimborges/go-socket.io/parser"
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
		url:       u.String(),
		events:    make(map[string]*eventHandler),
		writeChan: make(chan parser.Payload),
		quitChan:  make(chan struct{}),
	}, nil
}

// Connect dials the server and starts read/write loops.
func (c *Client) Connect() error {
	if err := c.connectOnce(); err != nil {
		return err
	}

	go c.readLoop()
	go c.writeLoop()

	return nil
}

// Close stops the client and closes the transport.
func (c *Client) Close() error {
	var err error

	c.closeOnce.Do(func() {
		close(c.quitChan)
		c.setConnected(false)

		if c.engineConn != nil {
			err = c.engineConn.Close()
		}
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

func (c *Client) connectOnce() error {
	dialer := engineio.Dialer{
		Transports: []transport.Transport{polling.Default},
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

func (c *Client) readLoop() {
	defer func() {
		c.setConnected(false)
		_ = c.Close()
	}()

	for {
		var header parser.Header
		var event string

		if err := c.decoder.DecodeHeader(&header, &event); err != nil {
			if c.onDisconnect != nil {
				c.onDisconnect(err)
			}

			return
		}

		if !c.validateNamespace(header.Namespace) {
			continue
		}

		var err error

		switch header.Type {
		case parser.Ack:
			err = c.decoder.DiscardLast()
		case parser.Connect:
			err = c.handleConnect()
		case parser.Disconnect:
			err = c.handleDisconnect()
		case parser.Event:
			err = c.handleEvent(event)
		}

		if err != nil {
			log.Printf("socketio: read error: %v", err)

			if c.onDisconnect != nil {
				c.onDisconnect(err)
			}

			return
		}
	}
}

func (c *Client) writeLoop() {
	for {
		select {
		case <-c.quitChan:
			return
		case pkg := <-c.writeChan:
			if err := c.encoder.Encode(pkg.Header, pkg.Data...); err != nil {
				log.Printf("socketio: write error: %v", err)

				if c.onDisconnect != nil {
					c.onDisconnect(err)
				}

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

	if c.onConnect != nil {
		c.onConnect()
	}

	return nil
}

func (c *Client) handleDisconnect() error {
	reason := ""

	args, err := c.decoder.DecodeArgs([]reflect.Type{reflect.TypeOf("")})
	if err != nil {
		return err
	}

	if len(args) > 0 {
		reason, _ = args[0].Interface().(string)
	}

	c.setConnected(false)

	if c.onDisconnect != nil {
		if reason != "" {
			c.onDisconnect(errDisconnectReason(reason))
		} else {
			c.onDisconnect(errServerDisconnect)
		}
	}

	return errServerDisconnect
}

func (c *Client) handleEvent(event string) error {
	handler, ok := c.getEventHandler(event)
	if !ok {
		return c.decoder.DiscardLast()
	}

	args, err := c.decoder.DecodeArgs(handler.argTypes)
	if err != nil {
		return err
	}

	return handler.Call(args)
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

type disconnectError string

func (e disconnectError) Error() string {
	return "socketio: disconnected: " + string(e)
}

var errServerDisconnect = errors.New("socketio: server disconnect")

func errDisconnectReason(reason string) error {
	return disconnectError(reason)
}
