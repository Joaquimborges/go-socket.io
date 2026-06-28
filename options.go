package socketio

import "net/http"

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
