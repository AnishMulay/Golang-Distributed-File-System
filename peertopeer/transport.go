package peertopeer

import "net"

// Peer represents one peer in the network
type Peer interface {
	RemoteAddress() net.Addr
	Close() error
}

// Transport handles communication between nodes(peers)
// Can be TCP, UDP, Websockets, etc
type Transport interface {
	Dial(address string) error
	ListenAndAccept() error
	Consume() <-chan RPC
	Close() error
}
