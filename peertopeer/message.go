package peertopeer

// Message represents any data which is sent from
// one peer to another over the network
type Message struct {
	Payload []byte
}
