package peertopeer

import "io"

type Decoder interface {
	Decode(io.Reader, any) error
}
