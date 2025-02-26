package peertopeer

import (
	"fmt"
	"log"
	"net"
	"sync"
)

type TCPTransport struct {
	listenAddress string
	listener      net.Listener

	mutex sync.RWMutex
	peers map[net.Addr]Peer
}

func NewTCPTransport(listenAddress string) *TCPTransport {
	return &TCPTransport{
		listenAddress: listenAddress,
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.listenAddress)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	fmt.Println("Handling connection from", conn.RemoteAddr())
}
