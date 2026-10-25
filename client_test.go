package socketio

import (
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewClientEmptyAddr(t *testing.T) {
	_, err := NewClient("")
	require.ErrorIs(t, err, ErrEmptyAddr)
}

func TestEmitNotConnected(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)

	err = client.Emit("event", "data")
	require.ErrorIs(t, err, ErrNotConnected)
}

func TestCloseBeforeConnect(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)

	require.NoError(t, client.Close())
}

func TestConnectTwiceReturnsErrAlreadyConnected(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)

	// connectOnce will fail without a server; simulate loopsStarted path via mutex state
	client.connectMu.Lock()
	client.loopsStarted = true
	client.connectMu.Unlock()

	err = client.Connect()
	require.ErrorIs(t, err, ErrAlreadyConnected)
}

func TestNewClientURL(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)
	require.Equal(t, "http://localhost:3000/socket.io/", client.url)
}

func TestNewClientWithHeaders(t *testing.T) {
	headers := http.Header{
		"Authorization": {"Bearer token"},
		"X-Custom":      {"value"},
	}

	client, err := NewClient("http://localhost:3000", WithHeaders(headers))
	require.NoError(t, err)
	require.Equal(t, headers, client.headers)

	headers.Set("Authorization", "mutated")
	require.Equal(t, "Bearer token", client.headers.Get("Authorization"))
}

func TestNewConnectError(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		message string
		data    string
	}{
		{"object", `{"message":"invalid service","data":{"code":1}}`, "invalid service", `{"code":1}`},
		{"object without data", `{"message":"invalid service"}`, "invalid service", ""},
		{"string", `"invalid service"`, "invalid service", ""},
		{"not json", `invalid service`, "invalid service", ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := newConnectError([]byte(test.raw))
			require.Equal(t, test.message, err.Message)
			require.Equal(t, test.data, string(err.Data))
			require.Equal(t, "socketio: connect error: "+test.message, err.Error())
		})
	}
}

func TestNewClientDefaultBackoff(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)
	require.Equal(t, time.Second, client.initialBackoff)
	require.Equal(t, 30*time.Second, client.maxBackoff)
	require.Zero(t, client.jitter)
}

func TestWithReconnectBackoffValidation(t *testing.T) {
	tests := []struct {
		name    string
		initial time.Duration
		max     time.Duration
		jitter  float64
		valid   bool
	}{
		{"valid", 200 * time.Millisecond, 2 * time.Second, 0.2, true},
		{"fixed", time.Second, time.Second, 0, true},
		{"full jitter", time.Second, time.Second, 1, true},
		{"zero initial", 0, time.Second, 0, false},
		{"negative initial", -time.Second, time.Second, 0, false},
		{"max below initial", 2 * time.Second, time.Second, 0, false},
		{"negative jitter", time.Second, time.Second, -0.1, false},
		{"jitter above one", time.Second, time.Second, 1.1, false},
		{"NaN jitter", time.Second, time.Second, math.NaN(), false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient("http://localhost:3000", WithReconnectBackoff(test.initial, test.max, test.jitter))
			if !test.valid {
				require.ErrorIs(t, err, ErrInvalidOption)
				require.Nil(t, client)

				return
			}

			require.NoError(t, err)
			require.Equal(t, test.initial, client.initialBackoff)
			require.Equal(t, test.max, client.maxBackoff)
			require.Equal(t, test.jitter, client.jitter)
		})
	}
}

func TestNextDelay(t *testing.T) {
	backoff := time.Second

	require.Equal(t, backoff, nextDelay(backoff, 0, 0.99), "no jitter is the backoff itself")
	require.Equal(t, backoff, nextDelay(backoff, 0.5, 0), "r=0 is the upper bound")
	require.Equal(t, 750*time.Millisecond, nextDelay(backoff, 0.5, 0.5))

	for _, jitter := range []float64{0.2, 0.5, 1} {
		for _, r := range []float64{0, 0.25, 0.5, 0.999999} {
			d := nextDelay(backoff, jitter, r)
			require.LessOrEqual(t, d, backoff)
			require.Greater(t, d, time.Duration(float64(backoff)*(1-jitter)))
		}
	}
}

func TestReconnectWithoutSession(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)

	require.ErrorIs(t, client.Reconnect(), ErrNotConnected)
	require.NoError(t, client.Close())
	require.ErrorIs(t, client.Reconnect(), ErrNotConnected)
}
