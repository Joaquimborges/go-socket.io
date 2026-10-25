package socketio

import (
	"fmt"
	"net/http"
	"time"
)

// ClientOption configures a Client at construction time.
type ClientOption func(*Client)

// WithHeaders sets HTTP headers sent on each connection attempt, including reconnects.
func WithHeaders(h http.Header) ClientOption {
	return func(c *Client) {
		if h != nil {
			c.headers = h.Clone()
		}
	}
}

// WithReconnectBackoff sets the wait between connection attempts. It starts at initial, doubles after each
// failed attempt up to maxBackoff, and returns to initial once a connection is accepted. With jitter above
// zero each wait is drawn between backoff×(1−jitter) and backoff, so it never exceeds maxBackoff.
//
// The default is 1s doubling to 30s with no jitter. initial must be positive, maxBackoff at least initial and
// jitter within [0, 1]; otherwise NewClient returns ErrInvalidOption.
func WithReconnectBackoff(initial, maxBackoff time.Duration, jitter float64) ClientOption {
	return func(c *Client) {
		if initial <= 0 || maxBackoff < initial || !(jitter >= 0 && jitter <= 1) {
			c.optErr = fmt.Errorf("%w: reconnect backoff initial=%s max=%s jitter=%v",
				ErrInvalidOption, initial, maxBackoff, jitter)

			return
		}

		c.initialBackoff = initial
		c.maxBackoff = maxBackoff
		c.jitter = jitter
	}
}

// WithEventErrorHandler keeps the session alive when an event handler panics or its payload does not decode
// into the handler's argument types. fn receives the event and the error, the ack is still sent when the
// server asked for one, and the next event is read on the same connection.
//
// Acknowledging an event that was not processed is deliberate: redelivery cannot fix a payload that fails
// every time, and the ack is what stops the server from redelivering it. A payload with binary attachments
// that fails to decode still ends the session, because its attachments cannot be skipped.
//
// Without this option, such an error ends the session without an ack and the client reconnects.
func WithEventErrorHandler(fn func(event string, err error)) ClientOption {
	return func(c *Client) {
		c.onEventError = fn
	}
}
