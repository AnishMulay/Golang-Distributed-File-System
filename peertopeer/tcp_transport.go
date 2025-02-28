package peertopeer

import (
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

type TCPTransportConfig struct {
	ListenAddress string
	HandShakeFunc HandShakeFunc
	Decoder       Decoder
}

// TCPTransport handles communication between nodes(peers) over TCP
type TCPTransport struct {
	TCPTransportConfig
	listener net.Listener

	mutex sync.RWMutex
	peers map[net.Addr]Peer
}

func NewTCPTransport(config TCPTransportConfig) *TCPTransport {
	return &TCPTransport{
		TCPTransportConfig: config,
	}
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.ListenAddress)
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
		}

		log.Println("New incoming connection from", conn)
		go t.handleConn(conn)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn) {
	peer := NewTCPPeer(conn, true)

	if err := t.HandShakeFunc(peer); err != nil {
		log.Println("Error shaking hands with peer:", err)
		conn.Close()
		return
	}

	// read loop
	msg := &Message{}
	for {
		if err := t.Decoder.Decode(conn, msg); err != nil {
			log.Println("TCP error decoding message:", err)
			continue
		}

		log.Println("Received message from", conn.RemoteAddr(), ":", msg)
	}
}
