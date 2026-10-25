package socketio

import (
	"encoding/json"
	"errors"
)

var (
	// ErrEmptyAddr is returned when NewClient receives an empty address.
	ErrEmptyAddr = errors.New("socketio: empty address")

	// ErrNotConnected is returned by Emit when the client is not connected.
	ErrNotConnected = errors.New("socketio: client not connected")

	// ErrAlreadyConnected is returned when Connect is called more than once.
	ErrAlreadyConnected = errors.New("socketio: client already connected")
)

// ConnectError is the server refusing the Socket.IO connection, typically from a middleware. It reaches
// OnReconnectAttempt like any other failed attempt, and the client retries with backoff.
type ConnectError struct {
	Message string
	Data    json.RawMessage
}

func (e *ConnectError) Error() string {
	return "socketio: connect error: " + e.Message
}

// newConnectError reads the CONNECT_ERROR payload: {"message", "data"} since Socket.IO v3, a bare string before.
func newConnectError(raw []byte) *ConnectError {
	var payload struct {
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &payload); err == nil {
		return &ConnectError{Message: payload.Message, Data: payload.Data}
	}

	var message string
	if err := json.Unmarshal(raw, &message); err == nil {
		return &ConnectError{Message: message}
	}

	return &ConnectError{Message: string(raw)}
}
