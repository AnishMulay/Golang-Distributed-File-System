package peertopeer

type HandShakeFunc func(Peer) error

func NOPEHandShakeFunc(Peer) error {
	return nil
}
