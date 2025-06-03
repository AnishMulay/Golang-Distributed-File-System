package main

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
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
	peerLock  sync.Mutex
	peers     map[string]peertopeer.Peer
	store     *Store
	pathStore *PathStore
	quitch    chan struct{}
}

func NewFileServer(config FileServerConfig) *FileServer {
	// Register all message types with gob
	RegisterMessageTypes()
	
	storeConfig := StoreConfig{
		Root:          config.StorageRoot,
		PathTransform: config.PathTransformFunc,
	}

	store := NewStore(storeConfig)

	pathStore, err := NewPathStore(config.StorageRoot + "_metadata")
	if err != nil {
		log.Fatalf("Failed to create PathStore: %v", err)
	}

	return &FileServer{
		FileServerConfig: config,
		store:            store,
		pathStore:        pathStore,
		quitch:           make(chan struct{}),
		peers:            make(map[string]peertopeer.Peer),
	}
}

func (s *FileServer) broadcast(msg *Message) error {
	log.Printf("[%s]: Broadcasting message of type %T\n", s.Transport.Addr(), msg.Payload)
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		return err
	}

	for _, peer := range s.peers {
		log.Printf("[%s]: Sending message to %s\n", s.Transport.Addr(), peer.RemoteAddr())
		peer.Send([]byte{peertopeer.IncomingMessage})
		if err := peer.Send(buf.Bytes()); err != nil {
			log.Printf("[%s]: Error sending to %s: %v\n", s.Transport.Addr(), peer.RemoteAddr(), err)
			continue // Continue with other peers even if one fails
		}
	}
	return nil
}

func (s *FileServer) OpenFile(path string, flags int, mode os.FileMode) (*OpenFile, error) {
	path = filepath.Clean(path)

	// Check if file exists
	exists := s.pathStore.Exists(path)

	// Handle O_CREATE|O_EXCL flags - atomic file creation
	if flags&O_CREATE != 0 && flags&O_EXCL != 0 {
		if exists {
			log.Printf("File %s exists, can't create with O_EXCL", path)
			return nil, os.ErrExist
		}

		// Create the file atomically
		log.Printf("Creating file %s with O_EXCL", path)
		if err := s.StoreFile(path, strings.NewReader(""), mode); err != nil {
			return nil, err
		}
	} else if flags&O_CREATE != 0 {
		// Create file if it doesn't exist
		if !exists {
			log.Printf("Creating file %s (didn't exist)", path)
			if err := s.StoreFile(path, strings.NewReader(""), mode); err != nil {
				return nil, err
			}
		}
	} else {
		// Open existing file
		if !exists {
			log.Printf("File %s not found", path)
			return nil, os.ErrNotExist
		}
	}

	// Handle O_TRUNC flag
	if exists && flags&O_TRUNC != 0 {
		log.Printf("Truncating file %s", path)
		if err := s.StoreFile(path, strings.NewReader(""), mode); err != nil {
			return nil, err
		}
	}

	log.Printf("Opened file %s with flags %d and mode %d", path, flags, mode)
	return NewOpenFile(s, path, flags, mode), nil
}

func (s *FileServer) Create(path string) (*OpenFile, error) {
	return s.OpenFile(path, O_RDWR|O_CREATE|O_TRUNC, 0666)
}

func (s *FileServer) Open(path string) (*OpenFile, error) {
	return s.OpenFile(path, O_RDONLY, 0)
}

func (s *FileServer) FileExists(path string) bool {
	return s.pathStore.Exists(path)
}

// GetFile gets a file by path
func (s *FileServer) GetFile(path string) (io.Reader, error) {
	path = filepath.Clean(path)

	meta, err := s.pathStore.Get(path)
	if err != nil {
		return nil, err
	}

	// Update access time
	meta.AccessTime = time.Now()
	s.pathStore.Set(path, meta.ContentKey, meta.Size, meta.Mode, meta.Type)

	// Get the file content using the content key
	return s.Get(meta.ContentKey)
}

// StoreFile stores a file at the given path
func (s *FileServer) StoreFile(path string, r io.Reader, mode os.FileMode) error {
	path = filepath.Clean(path)

	// Create a buffer to read the entire file
	buf := new(bytes.Buffer)
	tee := io.TeeReader(r, buf)

	// Generate a content key based on the path
	// In a real implementation, this would be a hash of the content
	contentKey := path

	// Store in the content-addressable store
	log.Printf("[%s]: Storing file (%s) in local store\n", s.Transport.Addr(), contentKey)
	size, err := s.store.Write(contentKey, tee)
	if err != nil {
		return err
	}

	// Store metadata
	fileType := FileTypeRegular
	if err := s.pathStore.Set(path, contentKey, size, mode, fileType); err != nil {
		return err
	}

	// Broadcast to peers
	msg := Message{
		Payload: MessageStoreFile{
			Key:  contentKey,
			Size: size + 16,    // Add space for encryption overhead
			Path: path,         // Add path to message
			Mode: uint32(mode), // Add mode to message
		},
	}

	if err := s.broadcast(&msg); err != nil {
		log.Println("Error broadcasting message:", err)
		return err
	}

	// Send the file to peers
	time.Sleep(5 * time.Millisecond)
	peers := []io.Writer{}
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}

	mw := io.MultiWriter(peers...)

	mw.Write([]byte{peertopeer.IncomingStream})

	n, err := copyEncrypt(s.EncryptionKey, buf, mw)
	if err != nil {
		return err
	}

	log.Printf("[%s]: Sent %d bytes to %d peers\n", s.Transport.Addr(), n, len(s.peers))
	return nil
}

// DeleteFile deletes a file at the given path
func (s *FileServer) DeleteFile(path string) error {
	// Check if the file exists
	meta, err := s.pathStore.Get(path)
	if err != nil {
		return err
	}

	// Delete the metadata
	if err := s.pathStore.Delete(path); err != nil {
		return err
	}

	// Delete from content store
	if err := s.store.Delete(meta.ContentKey); err != nil {
		return err
	}

	// Broadcast delete message to peers
	msg := Message{
		Payload: MessageDeleteFile{
			Path: path,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		log.Println("Error broadcasting delete message:", err)
		return err
	}

	return nil
}