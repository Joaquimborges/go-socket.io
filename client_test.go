package socketio

import (
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
