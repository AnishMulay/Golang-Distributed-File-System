package main

import (
	"log"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func main() {
	tcpconfig := peertopeer.TCPTransportConfig{
		ListenAddress: ":3000",
		HandShakeFunc: peertopeer.NOPEHandShakeFunc,
		Decoder:       peertopeer.DefaultDecoder{},
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
