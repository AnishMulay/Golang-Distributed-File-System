package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"os"

	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/config"
	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

// createServer creates a new file server with the given configuration
func createServer(cfg *config.Config) *FileServer {
	tcpTransportConfig := peertopeer.TCPTransportConfig{
		ListenAddress:    cfg.Network.ListenAddress,
		AdvertiseAddress: cfg.Network.AdvertiseAddress,
		HandShakeFunc:    peertopeer.NOPEHandShakeFunc,
		Decoder:          peertopeer.DefaultDecoder{},
		DialTimeout:      cfg.Network.DialTimeout,
		ReadTimeout:      cfg.Network.ReadTimeout,
		WriteTimeout:     cfg.Network.WriteTimeout,
		HandshakeTimeout: cfg.Network.HandshakeTimeout,
		MaxRetries:       cfg.Network.MaxRetries,
		RetryBackoff:     cfg.Network.RetryBackoff,
	}
	tcpTransport := peertopeer.NewTCPTransport(tcpTransportConfig)

	fileServerConfig := FileServerConfig{
		EncryptionKey:     newEncryptionKey(),
		StorageRoot:       cfg.Storage.StorageRoot,
		MetadataRoot:      cfg.Storage.MetadataRoot,
		PathTransformFunc: CASTransformFunc,
		Transport:         tcpTransport,
		BootstrapPeers:    cfg.PeerNodes,
		Mode:              cfg.Mode,
		Role:              cfg.Role,
		NodeID:            cfg.NodeID,
		LeaderNodes:       cfg.LeaderNodes,
		ReplicationFactor: cfg.Replication.Factor,
		AsyncReplication:  cfg.Replication.AsyncReplication,
		AsyncQueueSize:    cfg.Replication.AsyncQueueSize,
		ConsistencyLevel:  cfg.Replication.ConsistencyLevel,
		DefaultFileMode:   cfg.Storage.DefaultFileMode,
		MaxFileSize:       cfg.Storage.MaxFileSize,
	}

	s := NewFileServer(fileServerConfig)
	tcpTransport.OnPeer = s.OnPeer
	return s
}

func init() {
	// Register message types for gob encoding/decoding
	RegisterMessageTypes()
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
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

// printUsage prints the usage information for the CLI
func printUsage() {
	fmt.Println("Usage: dfs <command> [arguments]")
	fmt.Println("Commands:")
	fmt.Println("  server --config <config-file>")
	fmt.Println("  put <server-address> <local-file> <remote-path>")
	fmt.Println("  get <server-address> <remote-path> <local-file>")
	fmt.Println("  delete <server-address> <remote-path>")
	fmt.Println("  exists <server-address> <remote-path>")
	fmt.Println("  ls <server-address> <remote-dir>")
	fmt.Println("  touch <server-address> <remote-path>")
	fmt.Println("  cat <server-address> <remote-path>")
	fmt.Println("  mkdir <server-address> <remote-path> [--recursive]")
	fmt.Println("\nFor server configuration, see config/config.yaml.example")
}

func runServer() {
	var configPath string

	for i := 2; i < len(os.Args); i++ {
		if os.Args[i] == "--config" && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			i++
		}
	}

	if configPath == "" {
		log.Fatal("--config flag is required")
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	server := createServer(cfg)
	log.Printf("Starting server in %s mode as %s on %s\n", cfg.Mode, cfg.Role, cfg.Network.ListenAddress)
	log.Printf("Peer nodes: %v\n", cfg.PeerNodes)
	if cfg.Mode == config.SingleLeader && cfg.Role == config.Follower {
		log.Printf("Leader nodes: %v\n", cfg.LeaderNodes)
	}
	log.Fatal(server.Start())
}

// connectToPeer establishes a connection to a peer server
func connectToPeer(address string) (peertopeer.Peer, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}
	return peertopeer.NewTCPPeer(conn, true), nil
}

// sendRequest sends a message to a peer and returns the connection for receiving a response
func sendRequest(address string, payload interface{}) (peertopeer.Peer, error) {
	// Ensure message types are registered
	RegisterMessageTypes()

	peer, err := connectToPeer(address)
	if err != nil {
		return nil, err
	}

	msg := Message{Payload: payload}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		peer.Close()
		return nil, err
	}

	peer.Send([]byte{peertopeer.IncomingMessage})
	if err := peer.Send(buf.Bytes()); err != nil {
		peer.Close()
		return nil, err
	}

	return peer, nil
}

// sendCommand sends a message to a peer without expecting a response
func sendCommand(address string, payload interface{}) error {
	peer, err := sendRequest(address, payload)
	if err != nil {
		return err
	}
	defer peer.Close()
	return nil
}

// receiveResponse receives and decodes a response from a peer
func receiveResponse(peer peertopeer.Peer, response interface{}) error {
	// Make sure we've registered all types
	RegisterMessageTypes()

	// First decode the message wrapper
	var msg Message
	decoder := gob.NewDecoder(peer)

	log.Printf("Waiting for response...")
	if err := decoder.Decode(&msg); err != nil {
		return fmt.Errorf("error decoding message: %w", err)
	}
	log.Printf("Received response of type %T", msg.Payload)

	// Then extract the payload and convert it to the expected response type
	switch v := msg.Payload.(type) {
	case MessageGetFileResponse:
		if r, ok := response.(*MessageGetFileResponse); ok {
			*r = v
			return nil
		}
	case MessageDeleteFileResponse:
		if r, ok := response.(*MessageDeleteFileResponse); ok {
			*r = v
			return nil
		}
	case MessageFileExistsResponse:
		if r, ok := response.(*MessageFileExistsResponse); ok {
			*r = v
			return nil
		}
	case MessageTouchFileResponse:
		if r, ok := response.(*MessageTouchFileResponse); ok {
			*r = v
			return nil
		}
	case MessageCatFileResponse:
		if r, ok := response.(*MessageCatFileResponse); ok {
			*r = v
			return nil
		}
	case MessageLsDirectoryResponse:
		if r, ok := response.(*MessageLsDirectoryResponse); ok {
			*r = v
			return nil
		}
	case MessageMkdirResponse:
		if r, ok := response.(*MessageMkdirResponse); ok {
			*r = v
			return nil
		}
	}

	return fmt.Errorf("unexpected response type: %T", msg.Payload)
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
	peer, err := sendRequest(serverAddress, MessageGetFileContent{Path: remotePath})
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Wait for response
	var response MessageGetFileResponse
	if err := receiveResponse(peer, &response); err != nil {
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
	// Send the delete request
	peer, err := sendRequest(serverAddress, MessageDeleteFileContent{Path: remotePath})
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Wait for response
	var response MessageDeleteFileResponse
	if err := receiveResponse(peer, &response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	fmt.Printf("Successfully deleted %s\n", remotePath)
}

func runExists(serverAddress, remotePath string) {
	// Send the exists request
	peer, err := sendRequest(serverAddress, MessageFileExists{Path: remotePath})
	if err != nil {
		fmt.Println("false")
		os.Exit(1)
	}
	defer peer.Close()

	// Wait for response
	var response MessageFileExistsResponse
	if err := receiveResponse(peer, &response); err != nil {
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
	// Send the ls request
	peer, err := sendRequest(serverAddress, MessageLsDirectory{Path: remotePath})
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Wait for response
	var response MessageLsDirectoryResponse
	if err := receiveResponse(peer, &response); err != nil {
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
	// Send the mkdir request
	peer, err := sendRequest(serverAddress, MessageMkdir{
		Path:      path,
		Recursive: recursive,
	})
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Wait for response
	var response MessageMkdirResponse
	if err := receiveResponse(peer, &response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	fmt.Printf("Successfully created directory %s\n", path)
}

func runTouch(serverAddress, remotePath string) {
	// Send the touch request
	peer, err := sendRequest(serverAddress, MessageTouchFile{
		Path: remotePath,
		Mode: 0644,
	})
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	// Wait for response
	var response MessageTouchFileResponse
	if err := receiveResponse(peer, &response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	fmt.Printf("Successfully created empty file %s\n", remotePath)
}

func runCat(serverAddress, remotePath string) {
	// Register message types to ensure proper encoding/decoding
	RegisterMessageTypes()

	// Send the cat request
	peer, err := sendRequest(serverAddress, MessageCatFile{Path: remotePath})
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer peer.Close()

	log.Printf("Sent cat request for %s to %s, waiting for response...", remotePath, serverAddress)

	// Create a response object
	var response MessageCatFileResponse

	// Use the common receiveResponse function
	if err := receiveResponse(peer, &response); err != nil {
		log.Fatalf("Failed to decode response: %v", err)
	}

	if response.Error != "" {
		log.Fatalf("Server error: %s", response.Error)
	}

	// Write the content to stdout
	os.Stdout.Write(response.Content)
}
