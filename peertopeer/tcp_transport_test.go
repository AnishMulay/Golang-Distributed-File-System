package peertopeer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	listenAddress := ":4000"
	tr := NewTCPTransport(listenAddress)
	assert.Equal(t, listenAddress, tr.listenAddress)

	assert.Nil(t, tr.ListenAndAccept())
}
