package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"sync"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

type FileServerConfig struct {
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         peertopeer.Transport
	BootstrapPeers    []string
}

type FileServer struct {
	FileServerConfig
	peerLock sync.Mutex
	peers    map[string]peertopeer.Peer
	store    *Store
	quitch   chan struct{}
}

func NewFileServer(config FileServerConfig) *FileServer {
	storeConfig := StoreConfig{
		Root:          config.StorageRoot,
		PathTransform: config.PathTransformFunc,
	}
	return &FileServer{
		FileServerConfig: config,
		store:            NewStore(storeConfig),
		quitch:           make(chan struct{}),
		peers:            make(map[string]peertopeer.Peer),
	}
}

type Payload struct {
	Key  string
	Data []byte
}

func (s *FileServer) broadcast(payload *Payload) error {
	for _, peer := range s.peers {
		buf := new(bytes.Buffer)
		if err := gob.NewEncoder(buf).Encode(payload); err != nil {
			return err
		}

		if err := peer.Send(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileServer) StoreFile(key string, r io.Reader) error {
	buf := new(bytes.Buffer)
	tee := io.TeeReader(r, buf)

	if err := s.store.Write(key, tee); err != nil {
		return err
	}

	p := &Payload{
		Key:  key,
		Data: buf.Bytes(),
	}

	return s.broadcast(p)
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) OnPeer(p peertopeer.Peer) error {
	s.peerLock.Lock()
	defer s.peerLock.Unlock()

	s.peers[p.RemoteAddr().String()] = p
	log.Println("Connected to peer", p.RemoteAddr())
	return nil
}

func (s *FileServer) loop() {
	defer func() {
		log.Println("FileServer loop stopped because user requested to quit")
		s.Transport.Close()
	}()

	for {
		select {
		case msg := <-s.Transport.Consume():
			var p Payload
			err := gob.NewDecoder(bytes.NewReader(msg.Payload)).Decode(&p)
			if err != nil {
				log.Fatal("Failed to decode payload", err)
			}
			log.Println("Received payload", p.Key, p.Data)
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) bootstrapNetwork() error {
	for _, addr := range s.BootstrapPeers {
		if len(addr) == 0 {
			continue
		}
		go func(address string) {
			log.Println("Dialing", address)
			if err := s.Transport.Dial(address); err != nil {
				log.Println("Failed to dial", address, err)
			}
		}(addr)
	}

	return nil
}

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()
	s.loop()

	return nil
}
