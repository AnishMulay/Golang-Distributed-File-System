package peertopeer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTCPTransport(t *testing.T) {
	listenAddress := ":4000"

	tcpconfig := TCPTransportConfig{
		ListenAddress: listenAddress,
		HandShakeFunc: NOPEHandShakeFunc,
	}

	tr := NewTCPTransport(tcpconfig)
	assert.Equal(t, listenAddress, tr.ListenAddress)

	assert.Nil(t, tr.ListenAndAccept())
}
