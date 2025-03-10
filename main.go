package main

import (
	"io"
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
	s1 := makeServer(":3000")
	s2 := makeServer(":4000", ":3000")

	go func() {
		log.Fatal(s1.Start())
	}()

	time.Sleep(1 * time.Second)

	go s2.Start()
	time.Sleep(1 * time.Second)

	// file := bytes.NewReader([]byte("Main file content 222"))
	// s1.Store("anishkey", file)
	// time.Sleep(500 * time.Millisecond)

	r, err := s2.Get("anishkey")
	if err != nil {
		log.Println(err)
	}

	b, err := io.ReadAll(r)
	if err != nil {
		log.Println(err)
	}

	log.Println(string(b))
}
