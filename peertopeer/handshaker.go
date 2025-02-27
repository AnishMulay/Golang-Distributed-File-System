package peertopeer

type HandShakeFunc func(any) error

func NOPEHandShakeFunc(any) error {
	return nil
}
