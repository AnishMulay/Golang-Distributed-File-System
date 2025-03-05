package main

import (
	"log"
	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func main() {
	tcpTransportConfig := peertopeer.TCPTransportConfig{
		ListenAddress: ":3000",
		HandShakeFunc: peertopeer.NOPEHandShakeFunc,
		Decoder:       peertopeer.DefaultDecoder{},
	}

	tcpTransport := peertopeer.NewTCPTransport(tcpTransportConfig)

	fileServerConfig := FileServerConfig{
		StorageRoot:       "3000_store",
		PathTransformFunc: CASTransformFunc,
		Transport:         tcpTransport,
		BootstrapPeers:    []string{":3001"},
	}

	fileServer := NewFileServer(fileServerConfig)

	go func() {
		time.Sleep(3 * time.Second)
		fileServer.Stop()
	}()

	if err := fileServer.Start(); err != nil {
		log.Fatal(err)
	}
}
