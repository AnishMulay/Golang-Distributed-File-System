package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
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
	if len(os.Args) < 2 {
		fmt.Println("Usage: dfs <command> [arguments]")
		fmt.Println("Commands:")
		fmt.Println("  server --port <port> [--peers <peer1,peer2,...>]")
		fmt.Println("  put <local-file> <remote-path>")
		fmt.Println("  get <remote-path> <local-file>")
		fmt.Println("  delete <remote-path>")
		fmt.Println("  exists <remote-path>")
		fmt.Println("  ls <remote-dir>")
		fmt.Println("  touch <remote-path>")
		fmt.Println("  cat <remote-path>")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "server":
		runServer()
	case "put":
		if len(os.Args) != 4 {
			fmt.Println("Usage: dfs put <local-file> <remote-path>")
			os.Exit(1)
		}
		runPut(os.Args[2], os.Args[3])
	case "get":
		if len(os.Args) != 4 {
			fmt.Println("Usage: dfs get <remote-path> <local-file>")
			os.Exit(1)
		}
		runGet(os.Args[2], os.Args[3])
	case "delete":
		if len(os.Args) != 3 {
			fmt.Println("Usage: dfs delete <remote-path>")
			os.Exit(1)
		}
		runDelete(os.Args[2])
	case "exists":
		if len(os.Args) != 3 {
			fmt.Println("Usage: dfs exists <remote-path>")
			os.Exit(1)
		}
		runExists(os.Args[2])
	case "ls":
		if len(os.Args) != 3 {
			fmt.Println("Usage: dfs ls <remote-dir>")
			os.Exit(1)
		}
		runLs(os.Args[2])
	case "touch":
		if len(os.Args) != 3 {
			fmt.Println("Usage: dfs touch <remote-path>")
			os.Exit(1)
		}
		runTouch(os.Args[2])
	case "cat":
		if len(os.Args) != 3 {
			fmt.Println("Usage: dfs cat <remote-path>")
			os.Exit(1)
		}
		runCat(os.Args[2])
	default:
		fmt.Printf("Unknown command: %s\n", command)
		os.Exit(1)
	}
}

func runServer() {
	var port string
	var peers string

	// Parse flags
	for i := 2; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			port = os.Args[i+1]
			i++
		} else if os.Args[i] == "--peers" && i+1 < len(os.Args) {
			peers = os.Args[i+1]
			i++
		}
	}

	if port == "" {
		port = "3000" // Default port
	}

	listenAddress := ":" + port

	var nodes []string
	if peers != "" {
		nodes = strings.Split(peers, ",")
	}

	server := makeServer(listenAddress, nodes...)
	log.Printf("Starting server on %s with peers %v\n", listenAddress, nodes)
	log.Fatal(server.Start())
}

func connectToServer() *FileServer {
	// Connect to default server at :3000
	s := makeServer(":0", ":3000")
	go s.Start()
	time.Sleep(100 * time.Millisecond) // Allow time to connect
	return s
}

func runPut(localFile, remotePath string) {
	server := connectToServer()

	file, err := os.Open(localFile)
	if err != nil {
		log.Fatalf("Failed to open local file: %v", err)
	}
	defer file.Close()

	if err := server.StoreFile(remotePath, file, 0644); err != nil {
		log.Fatalf("Failed to put file: %v", err)
	}

	fmt.Printf("Successfully uploaded %s to %s\n", localFile, remotePath)
}

func runGet(remotePath, localFile string) {
	server := connectToServer()

	reader, err := server.GetFile(remotePath)
	if err != nil {
		log.Fatalf("Failed to get file: %v", err)
	}

	file, err := os.Create(localFile)
	if err != nil {
		log.Fatalf("Failed to create local file: %v", err)
	}
	defer file.Close()

	n, err := io.Copy(file, reader)
	if err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}

	fmt.Printf("Successfully downloaded %s to %s (%d bytes)\n", remotePath, localFile, n)
}

func runDelete(remotePath string) {
	server := connectToServer()

	if err := server.DeleteFile(remotePath); err != nil {
		log.Fatalf("Failed to delete file: %v", err)
	}

	fmt.Printf("Successfully deleted %s\n", remotePath)
}

func runExists(remotePath string) {
	server := connectToServer()

	exists := server.FileExists(remotePath)
	fmt.Printf("%v\n", exists)

	// Exit with status code 0 if file exists, 1 if not
	if !exists {
		os.Exit(1)
	}
}

func runLs(remotePath string) {
	server := connectToServer()

	entries, err := server.pathStore.ListDir(remotePath)
	if err != nil {
		log.Fatalf("Failed to list directory: %v", err)
	}

	for _, entry := range entries {
		fmt.Println(entry)
	}
}

func runTouch(remotePath string) {
	server := connectToServer()

	// Try to open with O_CREATE | O_EXCL first for atomic creation
	file, err := server.OpenFile(remotePath, O_CREATE|O_EXCL|O_WRONLY, 0644)
	if err != nil {
		if os.IsExist(err) {
			// File exists, just update timestamp
			meta, err := server.pathStore.Get(remotePath)
			if err != nil {
				log.Fatalf("Failed to get metadata: %v", err)
			}

			meta.ModTime = time.Now()
			meta.AccessTime = time.Now()

			if err := server.pathStore.Set(remotePath, meta.ContentKey, meta.Size, meta.Mode, meta.Type); err != nil {
				log.Fatalf("Failed to update timestamps: %v", err)
			}

			fmt.Printf("Updated timestamps for %s\n", remotePath)
			return
		}

		log.Fatalf("Failed to touch file: %v", err)
	}
	defer file.Close()

	fmt.Printf("Created empty file %s\n", remotePath)
}

func runCat(remotePath string) {
	server := connectToServer()

	reader, err := server.GetFile(remotePath)
	if err != nil {
		log.Fatalf("Failed to get file: %v", err)
	}

	if _, err := io.Copy(os.Stdout, reader); err != nil {
		log.Fatalf("Failed to output data: %v", err)
	}
}
