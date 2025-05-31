package main

import (
	"bytes"
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
	// Start servers as before
	s1 := makeServer(":3000")
	s2 := makeServer(":4000", ":3000")
	s3 := makeServer(":5000", ":3000", ":4000")

	go func() {
		log.Fatal(s1.Start())
	}()
	go func() {
		log.Fatal(s2.Start())
	}()
	time.Sleep(1 * time.Second)
	go s3.Start()
	time.Sleep(1 * time.Second)

	// Test new file operations with file paths
	testFilePath := "/test.txt"
	testContent := "Hello, distributed file system!"

	// 1. Store a file by path
	err := s3.StoreFile(testFilePath, bytes.NewReader([]byte(testContent)), 0644)
	if err != nil {
		log.Fatalf("StoreFile failed: %v", err)
	}
	log.Println("Stored file at path:", testFilePath)

	// 2. Check if the file exists
	exists := s3.FileExists(testFilePath)
	log.Printf("File exists at %s: %v", testFilePath, exists)

	// 3. Retrieve and print the file content
	reader, err := s3.GetFile(testFilePath)
	if err != nil {
		log.Fatalf("GetFile failed: %v", err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	log.Println("File content:", string(content))

	// check on peer s2
	existsOnPeer := s2.FileExists(testFilePath)
	log.Printf("File exists on peer s2 at %s: %v", testFilePath, existsOnPeer)

	// 4. List files in the root directory (if ListDir is implemented)
	entries, err := s3.pathStore.ListDir("/")
	if err != nil {
		log.Printf("Failed to list directory: %v", err)
	} else {
		log.Println("Files in root directory:", entries)
	}

	// 5. Delete the file
	err = s3.DeleteFile(testFilePath)
	if err != nil {
		log.Fatalf("DeleteFile failed: %v", err)
	}
	log.Println("Deleted file at path:", testFilePath)

	// 6. Check if the file still exists
	exists = s3.FileExists(testFilePath)
	log.Printf("File exists at %s: %v", testFilePath, exists)

	// (Optional) Keep the old test loop for backwards compatibility
	// (Uncomment if you want to keep testing content-addressable keys)
	/*
		for i := 0; i < 20; i++ {
			key := fmt.Sprintf("anishkey%d", i)
			file := bytes.NewReader([]byte(fmt.Sprintf("Main file content %d", i)))
			s3.Store(key, file)
			time.Sleep(500 * time.Millisecond)

			if err := s3.store.Delete(key); err != nil {
				log.Println(err)
			}

			r, err := s3.Get(key)
			if err != nil {
				log.Println(err)
			}

			b, err := io.ReadAll(r)
			if err != nil {
				log.Println(err)
			}

			log.Println(string(b))
		}
	*/
}
