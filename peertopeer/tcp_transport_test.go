package peertopeer

import (
	"net"
	"testing"
	"time"
)

func TestTCPTransportTimeoutsAndRetries(t *testing.T) {
	// Create a transport with timeouts and retries
	config := TCPTransportConfig{
		ListenAddress:    ":3000",
		AdvertiseAddress: "localhost:3000",
		HandShakeFunc:    NOPEHandShakeFunc,
		Decoder:          DefaultDecoder{},
		DialTimeout:      1 * time.Second,
		ReadTimeout:      2 * time.Second,
		WriteTimeout:     2 * time.Second,
		HandshakeTimeout: 1 * time.Second,
		MaxRetries:       2,
		RetryBackoff:     100 * time.Millisecond,
	}

	transport := NewTCPTransport(config)

	// Start the transport
	if err := transport.ListenAndAccept(); err != nil {
		t.Fatalf("Failed to start transport: %v", err)
	}
	defer transport.Close()

	// Test connection to non-existent peer (should retry)
	start := time.Now()
	err := transport.Dial("localhost:9999")
	duration := time.Since(start)

	// Should fail after retries
	if err == nil {
		t.Error("Expected error when connecting to non-existent peer")
	}

	// Should have taken approximately (MaxRetries + 1) * DialTimeout
	expectedDuration := time.Duration(config.MaxRetries+1) * config.DialTimeout
	if duration < expectedDuration {
		t.Errorf("Expected retry duration >= %v, got %v", expectedDuration, duration)
	}

	// Test connection timeout
	listener, err := net.Listen("tcp", ":3001")
	if err != nil {
		t.Fatalf("Failed to create test listener: %v", err)
	}
	defer listener.Close()

	// Accept connection but don't respond (should trigger handshake timeout)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Don't send any data, just hold the connection
		time.Sleep(5 * time.Second)
	}()

	start = time.Now()
	err = transport.Dial("localhost:3001")
	duration = time.Since(start)

	// Should fail with timeout
	if err == nil {
		t.Error("Expected timeout error")
	}

	// Should have taken approximately HandshakeTimeout
	if duration < config.HandshakeTimeout {
		t.Errorf("Expected timeout duration >= %v, got %v", config.HandshakeTimeout, duration)
	}
}

func TestTCPTransportReadWriteTimeouts(t *testing.T) {
	// Create two transports for testing
	config1 := TCPTransportConfig{
		ListenAddress:    ":3002",
		AdvertiseAddress: "localhost:3002",
		HandShakeFunc:    NOPEHandShakeFunc,
		Decoder:          DefaultDecoder{},
		ReadTimeout:      500 * time.Millisecond,
		WriteTimeout:     500 * time.Millisecond,
	}

	config2 := TCPTransportConfig{
		ListenAddress:    ":3003",
		AdvertiseAddress: "localhost:3003",
		HandShakeFunc:    NOPEHandShakeFunc,
		Decoder:          DefaultDecoder{},
		ReadTimeout:      500 * time.Millisecond,
		WriteTimeout:     500 * time.Millisecond,
	}

	transport1 := NewTCPTransport(config1)
	transport2 := NewTCPTransport(config2)

	// Start both transports
	if err := transport1.ListenAndAccept(); err != nil {
		t.Fatalf("Failed to start transport1: %v", err)
	}
	defer transport1.Close()

	if err := transport2.ListenAndAccept(); err != nil {
		t.Fatalf("Failed to start transport2: %v", err)
	}
	defer transport2.Close()

	// Connect transport1 to transport2
	if err := transport1.Dial(config2.ListenAddress); err != nil {
		t.Fatalf("Failed to connect transports: %v", err)
	}

	// Wait for connection to be established
	time.Sleep(100 * time.Millisecond)

	// Test read timeout
	done := make(chan struct{})
	go func() {
		select {
		case <-transport2.Consume():
			t.Error("Unexpected message received")
		case <-time.After(config2.ReadTimeout * 2):
			// Read timeout worked as expected
			close(done)
		}
	}()

	select {
	case <-done:
		// Test passed
	case <-time.After(5 * time.Second):
		t.Error("Test timed out")
	}
}