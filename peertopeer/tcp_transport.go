package peertopeer

import (
	"errors"
	"log"
	"net"
	"sync"
)

// TCPPeer represents a remote peer in the network (over an established TCP connection)
type TCPPeer struct {
	// this is the underlying TCP connection
	net.Conn

	// this defines the type of peer this is
	// if true, then this peer initiated the connection
	// if false, then this peer was dialed by another peer
	outBound bool
}

func NewTCPPeer(conn net.Conn, outBound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outBound: outBound,
	}
}

func (p *TCPPeer) Send(data []byte) error {
	_, err := p.Conn.Write(data)
	return err
}

// RemoteAddress implements the Peer interface
// It returns the remote address of the peer
func (p *TCPPeer) RemoteAddr() net.Addr {
	return p.Conn.RemoteAddr()
}

// Close implements the Peer interface
// Close closes the underlying TCP connection with the peer
func (p *TCPPeer) Close() error {
	return p.Conn.Close()
}

type TCPTransportConfig struct {
	ListenAddress string
	HandShakeFunc HandShakeFunc
	Decoder       Decoder
	OnPeer        func(Peer) error
}

// TCPTransport handles communication between nodes(peers) over TCP
type TCPTransport struct {
	TCPTransportConfig
	listener net.Listener
	rpcch    chan RPC

	mutex sync.RWMutex
	peers map[net.Addr]Peer
}

func NewTCPTransport(config TCPTransportConfig) *TCPTransport {
	return &TCPTransport{
		TCPTransportConfig: config,
		rpcch:              make(chan RPC),
	}
}

// Consume returns a channel which can be used to receive messages from other peers
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

// Dial implements the Transport interface
// It dials a remote peer at the given address
func (t *TCPTransport) Dial(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}

	go t.handleConn(conn, true)

	return nil
}

func (t *TCPTransport) ListenAndAccept() error {
	var err error

	t.listener, err = net.Listen("tcp", t.ListenAddress)
	if err != nil {
		return err
	}

	go t.startAcceptLoop()

	log.Println("TCP Transport Listening on", t.ListenAddress)

	return nil
}

func (t *TCPTransport) startAcceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			log.Println("Error accepting connection:", err)
		}
		go t.handleConn(conn, false)
	}
}

func (t *TCPTransport) handleConn(conn net.Conn, outbound bool) {
	peer := NewTCPPeer(conn, outbound)
	var err error
	defer func() {
		log.Println("Closing connection with", conn.RemoteAddr(), ":", err)
		conn.Close()
	}()

	if err := t.HandShakeFunc(peer); err != nil {
		return
	}

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			return
		}
	}

	// read loop
	rpc := RPC{}
	for {
		if err = t.Decoder.Decode(conn, &rpc); err != nil {
			return
		}

		rpc.From = conn.RemoteAddr()
		log.Println("Received message from", rpc.From, ":", rpc.Payload)
	}
}
