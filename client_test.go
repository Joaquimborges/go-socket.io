package socketio

import (
	"net/http"
	"testing"

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
