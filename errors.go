package socketio

import "errors"

var (
	// ErrEmptyAddr is returned when NewClient receives an empty address.
	ErrEmptyAddr = errors.New("socketio: empty address")

	// ErrNotConnected is returned by Emit when the client is not connected.
	ErrNotConnected = errors.New("socketio: client not connected")

	// ErrAlreadyConnected is returned when Connect is called more than once.
	ErrAlreadyConnected = errors.New("socketio: client already connected")
)
