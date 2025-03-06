package peertopeer

// Message represents any data which is sent from
// one peer to another over the network
type RPC struct {
	From    string
	Payload []byte
}
