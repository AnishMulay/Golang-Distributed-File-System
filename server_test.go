//go:build integration

// server_test.go
package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func TestFileServerIntegration(t *testing.T) {
	// Setup test environment
	tempRoot := t.TempDir()

	// Create test servers
	s1 := newTestServer(t, ":3000", tempRoot)
	s2 := newTestServer(t, ":4000", tempRoot, ":3000")

	// Wait for servers to start
	waitForPeerConnection(s1, 1)
	waitForPeerConnection(s2, 1)

	// Test StoreFile
	testContent := []byte("test content")
	err := s1.StoreFile("/test.txt", bytes.NewReader(testContent), 0644)
	if err != nil {
		t.Fatalf("StoreFile failed: %v", err)
	}

	// Test FileExists
	if !s2.FileExists("/test.txt") {
		t.Error("File not replicated to peer")
	}

	// Test GetFile
	reader, err := s2.GetFile("/test.txt")
	if err != nil {
		t.Fatalf("GetFile failed: %v", err)
	}

	content, _ := io.ReadAll(reader)
	if !bytes.Equal(content, testContent) {
		t.Errorf("Content mismatch: got %q want %q", content, testContent)
	}
}

func newTestServer(t *testing.T, addr string, root string, bootstrapPeers ...string) *FileServer {
	transport := peertopeer.NewTCPTransport(peertopeer.TCPTransportConfig{
		ListenAddress: addr,
		HandShakeFunc: peertopeer.NOPEHandShakeFunc,
		Decoder:       peertopeer.DefaultDecoder{},
	})

	config := FileServerConfig{
		EncryptionKey:     newEncryptionKey(),
		StorageRoot:       filepath.Join(root, addr+"_store"),
		PathTransformFunc: CASTransformFunc,
		Transport:         transport,
		BootstrapPeers:    bootstrapPeers, // Add bootstrap peers
	}

	server := NewFileServer(config)
	server.pathStore, _ = NewPathStore(filepath.Join(root, addr+"_meta"))

	transport.OnPeer = server.OnPeer
	return server
}

func waitForPeerConnection(s *FileServer, count int) {
	timeout := 10 * time.Second
	start := time.Now()
	for {
		s.peerLock.Lock()
		current := len(s.peers)
		s.peerLock.Unlock()

		if current >= count || time.Since(start) > timeout {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForServerReady(addr string) {
	timeout := 10 * time.Second
	start := time.Now()
	for {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		if time.Since(start) > timeout {
			log.Fatalf("Server %s not ready after %v", addr, timeout)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
