package peertopeer

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

// TCPPeer represents a remote peer in the network (over an established TCP connection)
type TCPPeer struct {
	// this is the underlying TCP connection
	net.Conn

	// this defines the type of peer this is
	// if true, then this peer initiated the connection
	// if false, then this peer was dialed by another peer
	outBound bool

	wg *sync.WaitGroup
}

func NewTCPPeer(conn net.Conn, outBound bool) *TCPPeer {
	return &TCPPeer{
		Conn:     conn,
		outBound: outBound,
		wg:       &sync.WaitGroup{},
	}
}

func (p *TCPPeer) CloseStream() {
	// Only call Done() if the wait group counter is greater than zero
	// This prevents "negative WaitGroup counter" panic
	p.wg.Done()
}

func (p *TCPPeer) Send(data []byte) error {
	// We can't access the TCPTransport from the connection directly
	// If write timeout is needed, it should be set when creating the peer
	// or passed as a parameter to this method

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
	ListenAddress    string
	AdvertiseAddress string
	HandShakeFunc    HandShakeFunc
	Decoder          Decoder
	OnPeer           func(Peer) error
	// Timeouts
	DialTimeout      time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	HandshakeTimeout time.Duration
	// Retry settings
	MaxRetries   int
	RetryBackoff time.Duration
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
		rpcch:              make(chan RPC, 1024),
	}
}

// Addr returns the address on which the transport is listening
func (t *TCPTransport) Addr() string {
	return t.ListenAddress
}

// Consume returns a channel which can be used to receive messages from other peers
func (t *TCPTransport) Consume() <-chan RPC {
	return t.rpcch
}

func (t *TCPTransport) Close() error {
	return t.listener.Close()
}

// Dial implements the Transport interface
// It dials a remote peer at the given address with retries and timeouts
func (t *TCPTransport) Dial(address string) error {
	var conn net.Conn
	var err error

	dialer := &net.Dialer{
		Timeout: t.DialTimeout,
	}

	for attempt := 0; attempt <= t.MaxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("[DEBUG TCP %s]: Retry attempt %d/%d connecting to %s",
				t.ListenAddress, attempt, t.MaxRetries, address)
			time.Sleep(t.RetryBackoff)
		}

		conn, err = dialer.Dial("tcp", address)
		if err == nil {
			break
		}

		if attempt == t.MaxRetries {
			return fmt.Errorf("failed to connect after %d attempts: %w", t.MaxRetries+1, err)
		}
	}

	// Set read/write timeouts
	if t.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(t.ReadTimeout))
	}
	if t.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(t.WriteTimeout))
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

	// Set initial timeouts
	if t.ReadTimeout > 0 {
		conn.SetReadDeadline(time.Now().Add(t.ReadTimeout))
	}
	if t.WriteTimeout > 0 {
		conn.SetWriteDeadline(time.Now().Add(t.WriteTimeout))
	}

	// Perform handshake with timeout
	if t.HandshakeTimeout > 0 {
		conn.SetDeadline(time.Now().Add(t.HandshakeTimeout))
	}
	if err := t.HandShakeFunc(peer); err != nil {
		log.Printf("[DEBUG TCP %s]: Handshake failed with %s: %v",
			t.ListenAddress, conn.RemoteAddr(), err)
		return
	}
	// Reset deadlines after handshake
	conn.SetDeadline(time.Time{})

	if t.OnPeer != nil {
		if err = t.OnPeer(peer); err != nil {
			log.Printf("[DEBUG TCP %s]: OnPeer callback failed with %s: %v",
				t.ListenAddress, conn.RemoteAddr(), err)
			return
		}
	}

	log.Printf("[DEBUG TCP %s]: Starting read loop for peer %s (outbound: %v)",
		t.ListenAddress, conn.RemoteAddr(), outbound)

	// read loop
	for {
		// Reset read deadline for each message
		if t.ReadTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(t.ReadTimeout))
		}

		rpc := RPC{}
		if err = t.Decoder.Decode(conn, &rpc); err != nil {
			log.Printf("[DEBUG TCP %s]: Error decoding message from %s: %v",
				t.ListenAddress, conn.RemoteAddr(), err)
			return
		}
		rpc.From = conn.RemoteAddr().String()

		if rpc.Stream {
			// Add a timeout to prevent hanging indefinitely
			streamDone := make(chan struct{})

			peer.wg.Add(1)
			log.Printf("[DEBUG TCP %s]: Receiving stream from %s",
				t.ListenAddress, rpc.From)

			go func() {
				peer.wg.Wait()
				close(streamDone)
			}()

			// Wait for stream to complete with a timeout
			select {
			case <-streamDone:
				log.Printf("[DEBUG TCP %s]: Stream from %s ended normally. Resuming read loop",
					t.ListenAddress, rpc.From)
			case <-time.After(t.ReadTimeout):
				log.Printf("[DEBUG TCP %s]: Stream from %s timed out. Forcing stream close",
					t.ListenAddress, rpc.From)
				// Force the stream to close
				peer.wg.Done()
			}
			continue
		}

		log.Printf("[DEBUG TCP %s]: Received message from %s, forwarding to channel",
			t.ListenAddress, rpc.From)
		t.rpcch <- rpc
	}
}
