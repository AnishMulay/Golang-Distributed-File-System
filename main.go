package main

import (
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
		EncryptionKey:     newEncryptionKey(),
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
	// s1 := makeServer(":3000")
	// s2 := makeServer(":4000", ":3000")
	// s3 := makeServer(":5000", ":3000", ":4000")

	// go func() {
	// 	log.Fatal(s1.Start())
	// 	time.Sleep(1 * time.Second)
	// }()

	// go func() {
	// 	log.Fatal(s2.Start())
	// 	time.Sleep(1 * time.Second)
	// }()

	// time.Sleep(1 * time.Second)

	// go s3.Start()
	// time.Sleep(1 * time.Second)

	// for i := 0; i < 20; i++ {
	// 	key := fmt.Sprintf("anishkey%d", i)
	// 	file := bytes.NewReader([]byte(fmt.Sprintf("Main file content %d", i)))
	// 	s3.Store(key, file)
	// 	time.Sleep(500 * time.Millisecond)

	// 	if err := s3.store.Delete(key); err != nil {
	// 		log.Println(err)
	// 	}

	// 	r, err := s3.Get(key)
	// 	if err != nil {
	// 		log.Println(err)
	// 	}

	// 	b, err := io.ReadAll(r)
	// 	if err != nil {
	// 		log.Println(err)
	// 	}

	// 	log.Println(string(b))
	// }

	s1 := makeServer(":3000")
	go s1.Start()
	select {}
}
