package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
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

	// Use the actual address as the key, but mark it as a peer in the value
	s.peers[p.RemoteAddr().String()] = p
	log.Println("Connected to peer", p.RemoteAddr())
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
	if s.store.Has(key) {
		log.Printf("[%s]: Key found in local store\n", s.Transport.Addr())
		_, r, err := s.store.Read(key)
		return r, err
	}

	log.Printf("[%s]: Key not found in local store, broadcasting request\n", s.Transport.Addr())

	msg := Message{
		Payload: MessageGetFile{
			Key: key,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		return nil, err
	}

	time.Sleep(500 * time.Millisecond)

	for _, peer := range s.peers {
		var fileSize int64
		binary.Read(peer, binary.LittleEndian, &fileSize)
		n, err := s.store.WriteDecryptStream(s.EncryptionKey, key, io.LimitReader(peer, fileSize))
		if err != nil {
			return nil, err
		}

		log.Printf("[%s]: Received %d bytes from %s\n", s.Transport.Addr(), n, peer.RemoteAddr())
		peer.CloseStream()
	}

	_, r, err := s.store.Read(key)
	return r, err
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