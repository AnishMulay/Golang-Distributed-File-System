package peertopeer

// Peer represents one peer in the network
type Peer interface {
	Close() error
}

// Transport handles communication between nodes(peers)
// Can be TCP, UDP, Websockets, etc
type Transport interface {
	ListenAndAccept() error
	Consume() <-chan RPC
}
