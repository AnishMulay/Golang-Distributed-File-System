package main

import (
	"log"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func OnPeer(p peertopeer.Peer) error {
	p.Close()
	return nil
}

func main() {
	tcpconfig := peertopeer.TCPTransportConfig{
		ListenAddress: ":3000",
		HandShakeFunc: peertopeer.NOPEHandShakeFunc,
		Decoder:       peertopeer.DefaultDecoder{},
		OnPeer:        OnPeer,
	}

	tr := peertopeer.NewTCPTransport(tcpconfig)

	go func() {
		for {
			msg := <-tr.Consume()
			log.Printf("Received message from %s: %s", msg.From, msg.Payload)
		}
	}()

	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	select {}
}
