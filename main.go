package main

import (
	"log"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func main() {
	tcpconfig := peertopeer.TCPTransportConfig{
		ListenAddress: ":3000",
		HandShakeFunc: peertopeer.NOPEHandShakeFunc,
		Decoder:       peertopeer.GOBDecoder{},
	}

	tr := peertopeer.NewTCPTransport(tcpconfig)
	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	select {}
}
