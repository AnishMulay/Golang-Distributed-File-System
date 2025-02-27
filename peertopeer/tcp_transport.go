package peertopeer

import (
	"fmt"
	"log"
	"net"
	"sync"
)

// TCPPeer represents a remote peer in the network (over an established TCP connection)
type TCPPeer struct {
	// conn is the underlying TCP connection
	conn net.Conn

	// this defines the type of peer this is
	// if true, then this peer initiated the connection
	// if false, then this peer was dialed by another peer
	outBound bool
}

func NewTCPPeer(conn net.Conn, outBound bool) *TCPPeer {
	return &TCPPeer{
		conn:     conn,
		outBound: outBound,
	}
}

// TCPTransport handles communication between nodes(peers) over TCP
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
	peer := NewTCPPeer(conn, true)

	fmt.Println("Handling connection from", peer)
}
