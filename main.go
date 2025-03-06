package main

import (
	"bytes"
	"log"
	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func makeServer(listenAddress string, nodes ...string) *FileServer {
	tcpTransposrtConfig := peertopeer.TCPTransportConfig{
		ListenAddress: listenAddress,
		HandShakeFunc: peertopeer.NOPEHandShakeFunc,
		Decoder:       peertopeer.DefaultDecoder{},
	}

	tcpTransport := peertopeer.NewTCPTransport(tcpTransposrtConfig)

	fileServerConfig := FileServerConfig{
		StorageRoot:       listenAddress + "_store",
		PathTransformFunc: CASTransformFunc,
		Transport:         tcpTransport,
		BootstrapPeers:    nodes,
	}

	s := NewFileServer(fileServerConfig)
	tcpTransport.OnPeer = s.OnPeer
	return s
}

func main() {
	s1 := makeServer(":3000", "")
	s2 := makeServer(":4000", ":3000")

	go func() {
		log.Fatal(s1.Start())
	}()

	go s2.Start()
	time.Sleep(1 * time.Second)

	file := bytes.NewReader([]byte("Hello World"))
	s2.StoreFile("anishkey", file)

	select {}
}
