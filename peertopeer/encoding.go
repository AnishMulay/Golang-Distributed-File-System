package peertopeer

import (
	"encoding/gob"
	"io"

	"google.golang.org/protobuf/proto"
)

type Decoder interface {
	Decode(io.Reader, *Rpc) error
}

type GOBDecoder struct{}

func (dec GOBDecoder) Decode(r io.Reader, msg *Rpc) error {
	return gob.NewDecoder(r).Decode(msg)
}

type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader, msg *Rpc) error {
	// Handle stream header detection
	peekBuf := make([]byte, 1)
	if _, err := r.Read(peekBuf); err != nil {
		return err
	}

	msg.Stream = peekBuf[0] == IncomingStream
	if msg.Stream {
		return nil
	}

	// Read and decode full protobuf message
	buf, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	return proto.Unmarshal(buf, msg)
}

type ProtobufDecoder struct{}

func (dec ProtobufDecoder) Decode(r io.Reader, msg *Rpc) error {
	buf := make([]byte, 1028)
	n, err := r.Read(buf)
	if err != nil {
		return err
	}

	return proto.Unmarshal(buf[:n], msg)
}
