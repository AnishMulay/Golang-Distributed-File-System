package peertopeer

import (
	"encoding/gob"
	"io"
	"log"
)

type Decoder interface {
	Decode(io.Reader, *RPC) error
}

type GOBDecoder struct{}

func (dec GOBDecoder) Decode(r io.Reader, msg *RPC) error {
	return gob.NewDecoder(r).Decode(msg)
}

type DefaultDecoder struct{}

func (dec DefaultDecoder) Decode(r io.Reader, msg *RPC) error {
	peekBuf := make([]byte, 1)
	if _, err := r.Read(peekBuf); err != nil {
		return err
	}

	// Check if the message is a stream
	// If it is a stream, we dont need to decode the payload
	stream := peekBuf[0] == IncomingStream
	if stream {
		msg.Stream = true
		log.Printf("[DEBUG DECODER]: Detected incoming stream")
		return nil
	}

	if peekBuf[0] != IncomingMessage {
		log.Printf("[DEBUG DECODER]: Unexpected message type: %d (expected %d)",
			peekBuf[0], IncomingMessage)
	}

	buf := make([]byte, 1028)
	n, err := r.Read(buf)
	if err != nil {
		log.Printf("[DEBUG DECODER]: Error reading message payload: %v", err)
		return err
	}

	log.Printf("[DEBUG DECODER]: Read %d bytes of message payload", n)
	msg.Payload = buf[:n]

	return nil
}
