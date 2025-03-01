package peertopeer

import "net"

// Message represents any data which is sent from
// one peer to another over the network
type RPC struct {
	From    net.Addr
	Payload []byte
}
