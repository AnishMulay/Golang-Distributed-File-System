package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
	"google.golang.org/protobuf/proto"
)

type FileServerConfig struct {
	EncryptionKey     []byte
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

func (s *FileServer) broadcast(msg *peertopeer.Rpc) error {
	log.Println("Broadcasting message")

	buf, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	for _, peer := range s.peers {
		msgType := []byte{peertopeer.IncomingMessage}
		if msg.GetStream() {
			msgType = []byte{peertopeer.IncomingStream}
		}

		if err := peer.Send(msgType); err != nil {
			return err
		}

		if err := peer.Send(buf); err != nil {
			return err
		}
	}

	log.Println("Broadcasted message to", len(s.peers), "peers")

	return nil
}

type Message struct {
	*peertopeer.Rpc
}

type MessageStoreFile struct {
	Key  string
	Size int64
}

type MessageGetFile struct {
	Key string
}

func (s *FileServer) Get(key string) (io.Reader, error) {
	if s.store.Has(key) {
		log.Printf("[%s]: Key found in local store\n", s.Transport.Addr())
		_, r, err := s.store.Read(key)
		return r, err
	}

	log.Printf("[%s]: Key not found in local store, broadcasting request\n", s.Transport.Addr())

	msg := &peertopeer.Rpc{
		Payload: &peertopeer.Rpc_GetFile{
			GetFile: &peertopeer.MessageGetFile{
				Key: hashKey(key),
			},
		},
	}

	if err := s.broadcast(msg); err != nil {
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

func (s *FileServer) Store(key string, r io.Reader) error {
	log.Printf("[%s]: Storing file (%s) in local store\n", s.Transport.Addr(), key)
	var (
		fileBuf = new(bytes.Buffer)
		tee     = io.TeeReader(r, fileBuf)
	)
	size, err := s.store.Write(key, tee)
	if err != nil {
		return err
	}

	msg := &peertopeer.Rpc{
		Payload: &peertopeer.Rpc_StoreFile{
			StoreFile: &peertopeer.MessageStoreFile{
				Key:  hashKey(key),
				Size: size + 16,
			},
		},
	}

	if err := s.broadcast(msg); err != nil {
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
		log.Println("FileServer loop stopped because or an error or quitch")
		s.Transport.Close()
	}()

	for {
		select {
		case rpc := <-s.Transport.Consume():
			if err := s.handleRPC(&rpc); err != nil {
				log.Println("Error handling RPC:", err)
			}
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) handleRPC(rpc *peertopeer.Rpc) error {
	switch payload := rpc.Payload.(type) {
	case *peertopeer.Rpc_StoreFile:
		return s.handleStoreFile(rpc.From, payload.StoreFile)
	case *peertopeer.Rpc_GetFile:
		return s.handleGetFile(rpc.From, payload.GetFile)
	default:
		return fmt.Errorf("unknown RPC type: %T", payload)
	}
}

func (s *FileServer) handleGetFile(from string, msg *peertopeer.MessageGetFile) error {
	key := msg.GetKey()

	if !s.store.Has(key) {
		return fmt.Errorf("[%s] file with key '%s' not found", s.Transport.Addr(), key)
	}

	log.Printf("[%s] serving file (key: %s) to %s\n", s.Transport.Addr(), key, from)

	// Get the file from local store
	fileSize, r, err := s.store.Read(key)
	if err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}
	defer func() {
		if closer, ok := r.(io.Closer); ok {
			closer.Close()
		}
	}()

	// Get the peer connection
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not found in peer map", from)
	}

	// Send stream initialization header
	if err := peer.Send([]byte{peertopeer.IncomingStream}); err != nil {
		return fmt.Errorf("error sending stream header: %w", err)
	}

	// Send file size using binary encoding (preserve existing protocol)
	if err := binary.Write(peer, binary.LittleEndian, fileSize); err != nil {
		return fmt.Errorf("error sending file size: %w", err)
	}

	// Create protobuf message for each chunk (adjust chunk size as needed)
	const chunkSize = 4096
	buf := make([]byte, chunkSize)

	for {
		n, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading file chunk: %w", err)
		}

		// Create and send chunk message
		chunkMsg := &peertopeer.Rpc{
			From: s.Transport.Addr(),
			Payload: &peertopeer.Rpc_FileChunk{
				FileChunk: &peertopeer.FileChunk{
					Content: buf[:n],
				},
			},
			Stream: true,
		}

		data, err := proto.Marshal(chunkMsg)
		if err != nil {
			return fmt.Errorf("error marshaling chunk: %w", err)
		}

		if err := peer.Send(data); err != nil {
			return fmt.Errorf("error sending chunk: %w", err)
		}
	}

	log.Printf("[%s] successfully sent %d bytes to %s", s.Transport.Addr(), fileSize, from)
	return nil
}

func (s *FileServer) handleStoreFile(from string, msg *peertopeer.MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s not found", from)
	}

	n, err := s.store.Write(msg.Key, io.LimitReader(peer, msg.Size))
	if err != nil {
		return err
	}

	log.Printf("[%s] Stored %d bytes\n", s.Transport.Addr(), n)
	peer.CloseStream()
	return nil
}

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

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.bootstrapNetwork()
	s.loop()

	return nil
}

func init() {
	gob.Register(MessageStoreFile{})
	gob.Register(MessageGetFile{})
}
