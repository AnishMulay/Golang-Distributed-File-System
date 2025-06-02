package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
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
		fmt.Println("  put <server-address> <local-file> <remote-path>")
		fmt.Println("  get <server-address> <remote-path> <local-file>")
		fmt.Println("  delete <server-address> <remote-path>")
		fmt.Println("  exists <server-address> <remote-path>")
		fmt.Println("  ls <server-address> <remote-dir>")
		fmt.Println("  touch <server-address> <remote-path>")
		fmt.Println("  cat <server-address> <remote-path>")
		fmt.Println("  mkdir <server-address> <remote-path> [--recursive]")
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "server":
		runServer()
	default:
		if len(os.Args) < 3 {
			fmt.Println("You must specify a server address for this command")
			os.Exit(1)
		}
		serverAddress := os.Args[2]
		args := os.Args[3:]

		switch command {
		case "put":
			if len(args) != 2 {
				fmt.Println("Usage: dfs put <server-address> <local-file> <remote-path>")
				os.Exit(1)
			}
			runPut(serverAddress, args[0], args[1])
		case "get":
			if len(args) != 2 {
				fmt.Println("Usage: dfs get <server-address> <remote-path> <local-file>")
				os.Exit(1)
			}
			runGet(serverAddress, args[0], args[1])
		case "delete":
			if len(args) != 1 {
				fmt.Println("Usage: dfs delete <server-address> <remote-path>")
				os.Exit(1)
			}
			runDelete(serverAddress, args[0])
		case "exists":
			if len(args) != 1 {
				fmt.Println("Usage: dfs exists <server-address> <remote-path>")
				os.Exit(1)
			}
			runExists(serverAddress, args[0])
		case "ls":
			if len(args) != 1 {
				fmt.Println("Usage: dfs ls <server-address> <remote-dir>")
				os.Exit(1)
			}
			runLs(serverAddress, args[0])
		case "touch":
			if len(args) != 1 {
				fmt.Println("Usage: dfs touch <server-address> <remote-path>")
				os.Exit(1)
			}
			runTouch(serverAddress, args[0])
		case "cat":
			if len(args) != 1 {
				fmt.Println("Usage: dfs cat <server-address> <remote-path>")
				os.Exit(1)
			}
			runCat(serverAddress, args[0])
		case "mkdir":
			if len(args) < 1 {
				fmt.Println("Usage: dfs mkdir <server-address> <remote-path> [--recursive]")
				os.Exit(1)
			}
			recursive := len(args) >= 2 && args[1] == "--recursive"
			runMkdir(serverAddress, args[0], recursive)
		default:
			fmt.Printf("Unknown command: %s\n", command)
			os.Exit(1)
		}
	}
}

func runServer() {
	var port string
	var peers string

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
		port = "3000"
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

func connectToPeer(address string) (peertopeer.Peer, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	return peertopeer.NewTCPPeer(conn, true), nil
}

func sendCommand(address string, payload interface{}) error {
	peer, err := connectToPeer(address)
	if err != nil {
		return err
	}
	defer peer.Close()

	msg := Message{Payload: payload}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	return peer.Send(buf.Bytes())
}

func runPut(serverAddress, localFile, remotePath string) {
	// Read the local file
	fileData, err := os.ReadFile(localFile)
	if err != nil {
		log.Fatalf("Failed to read local file: %v", err)
	}

	// Send a single message containing both the file metadata and content
	err = sendCommand(serverAddress, MessagePutFile{
		Path:    remotePath,
		Mode:    0644,
		Content: fileData,
	})
	if err != nil {
		log.Fatalf("Failed to upload file: %v", err)
	}

	fmt.Printf("Successfully uploaded %s to %s\n", localFile, remotePath)
}

func runGet(serverAddress, remotePath, localFile string) {
	// Send a request for the file
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Send the request message
	msg := Message{Payload: MessageGetFileContent{Path: remotePath}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}

	// Wait for response
	var response MessageGetFileResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	// Write the file content to the local file
	if err := os.WriteFile(localFile, response.Content, os.FileMode(response.Mode)); err != nil {
		log.Fatalf("Failed to write local file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s to %s (%d bytes)\n", remotePath, localFile, len(response.Content))
}

func runDelete(serverAddress, remotePath string) {
	// Connect to the server
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Send the delete request
	msg := Message{Payload: MessageDeleteFileContent{Path: remotePath}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}

	// Wait for response
	var response MessageDeleteFileResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	fmt.Printf("Successfully deleted %s\n", remotePath)
}

func runExists(serverAddress, remotePath string) {
	// Connect to the server
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		fmt.Println("false")
		os.Exit(1)
	}
	defer peer.Close()

	// Send the exists request
	msg := Message{Payload: MessageFileExists{Path: remotePath}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		fmt.Println("false")
		os.Exit(1)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		fmt.Println("false")
		os.Exit(1)
	}

	// Wait for response
	var response MessageFileExistsResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		fmt.Println("false")
		os.Exit(1)
	}

	if response.Error != "" {
		fmt.Println("false")
		os.Exit(1)
	}

	fmt.Println(response.Exists)
	if response.Exists {
		os.Exit(0)
	} else {
		os.Exit(1)
	}
}

func runLs(serverAddress, remotePath string) {
	// Connect to the server
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Send the ls request
	msg := Message{Payload: MessageLsDirectory{Path: remotePath}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}

	// Wait for response
	var response MessageLsDirectoryResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	// Display the directory listing
	if len(response.Files) == 0 {
		fmt.Println("Directory is empty")
		return
	}

	// Format and print the directory listing
	for _, file := range response.Files {
		modTime := time.Unix(file.ModTime, 0).Format("Jan 02 15:04")
		fileType := "-"
		if file.IsDir {
			fileType = "d"
		}
		mode := os.FileMode(file.Mode).String()
		fmt.Printf("%s%s %8d %s %s\n", fileType, mode[1:], file.Size, modTime, file.Name)
	}
}

func runMkdir(serverAddress, path string, recursive bool) {
	// Connect to the server
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Send the mkdir request
	msg := Message{Payload: MessageMkdir{
		Path:      path,
		Recursive: recursive,
	}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}

	// Wait for response
	var response MessageMkdirResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	fmt.Printf("Successfully created directory %s\n", path)
}

func runTouch(serverAddress, remotePath string) {
	// Connect to the server
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Send the touch request
	msg := Message{Payload: MessageTouchFile{
		Path: remotePath,
		Mode: 0644,
	}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}

	// Wait for response
	var response MessageTouchFileResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	fmt.Printf("Successfully created empty file %s\n", remotePath)
}

func runCat(serverAddress, remotePath string) {
	// Connect to the server
	peer, err := connectToPeer(serverAddress)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Send the cat request
	msg := Message{Payload: MessageCatFile{Path: remotePath}}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Fatalf("Failed to encode message: %v", err)
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Fatalf("Failed to send request: %v", err)
	}

	// Wait for response
	var response MessageCatFileResponse
	decoder := gob.NewDecoder(peer)
	if err := decoder.Decode(&response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	// Write the content to stdout
	os.Stdout.Write(response.Content)
}
