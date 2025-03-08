package peertopeer

import "net"

// Peer represents one peer in the network
type Peer interface {
	net.Conn
	Send([]byte) error
}

// Transport handles communication between nodes(peers)
// Can be TCP, UDP, Websockets, etc
type Transport interface {
	Addr() string
	Dial(address string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}
