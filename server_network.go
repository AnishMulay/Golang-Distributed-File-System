package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

// Start starts the file server
func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()
	s.loop()

	return nil
}

// Stop stops the file server
func (s *FileServer) Stop() {
	close(s.quitch)
}

// OnPeer is called when a new peer connects
func (s *FileServer) OnPeer(p peertopeer.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	addr := p.RemoteAddr().String()
	
	// Use the actual address as the key
	s.peers[addr] = p
	
	log.Printf("[DEBUG PEER %s]: Connected to peer %s, total peers: %d", 
		s.Transport.Addr(), addr, len(s.peers))
	
	// Log all current peers for debugging
	log.Printf("[DEBUG PEER %s]: Current peers:", s.Transport.Addr())
	for peerAddr := range s.peers {
		log.Printf("[DEBUG PEER %s]:   - %s", s.Transport.Addr(), peerAddr)
	}
	
	return nil
}

// loop processes incoming messages
func (s *FileServer) loop() {
	defer func() {
		log.Println("FileServer loop stopped because or an error or quitch")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			var msg Message
			if err := gob.NewDecoder(bytes.NewReader(rpc.Payload)).Decode(&msg); err != nil {
				log.Println("Error decoding message", err)
			}
			if err := s.handleMessage(rpc.From, &msg); err != nil {
				log.Println("Error handling message", err)
			}
		case <-s.quitch:
			return
		}
	}
}

// bootstrapNetwork connects to bootstrap peers
func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapPeers {
		if len(addr) == 0 {
			continue
		}
		go func(address string) {
			log.Printf("[%s]: Dialing %s\n", s.Transport.Addr(), address)
			if err := s.Transport.Dial(address); err != nil {
				log.Println("Failed to dial", address, err)
			}
		}(addr)
	}

	return nil
}

// Get retrieves a file from the network by key
func (s *FileServer) Get(key string) (io.Reader, error) {
	log.Printf("[DEBUG GET %s]: Starting Get operation for key %s", s.Transport.Addr(), key)

	if s.store.Has(key) {
		log.Printf("[DEBUG GET %s]: Key %s found in local store", s.Transport.Addr(), key)
		_, r, err := s.store.Read(key)
		if err != nil {
			log.Printf("[DEBUG GET %s]: Error reading from local store: %v", s.Transport.Addr(), err)
			return nil, err
		}
		log.Printf("[DEBUG GET %s]: Successfully read key %s from local store", s.Transport.Addr(), key)
		return r, err
	}

	log.Printf("[DEBUG GET %s]: Key %s not found in local store, broadcasting request to %d peers",
		s.Transport.Addr(), key, len(s.peers))

	msg := Message{
		Payload: MessageGetFile{
			Key: key,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		log.Printf("[DEBUG GET %s]: Error broadcasting request: %v", s.Transport.Addr(), err)
		return nil, err
	}

	log.Printf("[DEBUG GET %s]: Waiting for responses from peers...", s.Transport.Addr())
	time.Sleep(500 * time.Millisecond)

	peerCount := len(s.peers)
	if peerCount == 0 {
		log.Printf("[DEBUG GET %s]: No peers available to get file from", s.Transport.Addr())
		return nil, fmt.Errorf("no peers available to get file from")
	}

	log.Printf("[DEBUG GET %s]: Processing responses from %d peers", s.Transport.Addr(), peerCount)
	for addr, peer := range s.peers {
		log.Printf("[DEBUG GET %s]: Reading from peer %s", s.Transport.Addr(), addr)

		var fileSize int64
		err := binary.Read(peer, binary.LittleEndian, &fileSize)
		if err != nil {
			log.Printf("[DEBUG GET %s]: Error reading file size from peer %s: %v", s.Transport.Addr(), addr, err)
			continue
		}

		log.Printf("[DEBUG GET %s]: Received file size %d from peer %s", s.Transport.Addr(), fileSize, addr)

		n, err := s.store.WriteDecryptStream(s.EncryptionKey, key, io.LimitReader(peer, fileSize))
		if err != nil {
			log.Printf("[DEBUG GET %s]: Error writing to local store: %v", s.Transport.Addr(), err)
			continue
		}

		log.Printf("[DEBUG GET %s]: Received %d bytes from %s", s.Transport.Addr(), n, peer.RemoteAddr())
		peer.CloseStream()
		log.Printf("[DEBUG GET %s]: Closed stream from peer %s", s.Transport.Addr(), addr)
	}

	if s.store.Has(key) {
		log.Printf("[DEBUG GET %s]: Successfully retrieved key %s from network", s.Transport.Addr(), key)
		_, r, err := s.store.Read(key)
		return r, err
	}

	log.Printf("[DEBUG GET %s]: Failed to retrieve key %s from network", s.Transport.Addr(), key)
	return nil, fmt.Errorf("failed to retrieve key %s from network", key)
}

// Store stores a file in the network
func (s *FileServer) Store(key string, r io.Reader) error {
	log.Printf("[%s]: Storing file (%s) in local store\n", s.Transport.Addr(), key)
	var (
		fileBuf = new(bytes.Buffer)
		tee     = io.TeeReader(r, fileBuf)
	)

	log.Printf("[%s]: Storing file (%s) in local store\n", s.Transport.Addr(), key)
	size, err := s.store.Write(key, tee)
	if err != nil {
		return err
	}

	msg := Message{
		Payload: MessageStoreFile{
			Key:  key,
			Size: size + 16,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		log.Println("Error broadcasting message", err)
	}

	time.Sleep(5 * time.Millisecond)

	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	mw := io.MultiWriter(peers...)
	mw.Write([]byte{peertopeer.IncomingStream})
	n, err := copyEncrypt(s.EncryptionKey, fileBuf, mw)
	if err != nil {
		return err
	}

	log.Printf("[%s]: Sent %d bytes to %d peers\n", s.Transport.Addr(), n, len(s.peers))

	return nil
}
