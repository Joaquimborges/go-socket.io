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

func TestNewClientURL(t *testing.T) {
	client, err := NewClient("http://localhost:3000")
	require.NoError(t, err)
	require.Equal(t, "http://localhost:3000/socket.io/", client.url)
}
