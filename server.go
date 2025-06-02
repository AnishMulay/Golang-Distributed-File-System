package main

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
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
	pathStore *PathStore // Add PathStore
	quitch    chan struct{}
}

func NewFileServer(config FileServerConfig) *FileServer {
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
			return err
		}
	}
	return nil
}

type Message struct {
	Payload any
}

type MessageStoreFile struct {
	Key  string
	Size int64
	Path string // Add path
	Mode uint32 // Add mode
}

type MessageGetFile struct {
	Key string
}

type MessageDeleteFile struct {
	Path string
}

type MessageOpenFile struct {
	Path  string
	Flags int
	Mode  uint32
}

type MessageReadFile struct {
	Path   string
	Offset int64
	Length int
}

type MessageWriteFile struct {
	Path   string
	Offset int64
	Data   []byte
}

type MessageCloseFile struct {
	Path string
}

type MessageMkdir struct {
	Path      string
	Recursive bool
}

type MessagePutFile struct {
	Path    string
	Mode    uint32
	Content []byte
}

type MessageGetFileContent struct {
	Path string
}

type MessageGetFileResponse struct {
	Content []byte
	Mode    uint32
	Error   string
}

type MessageDeleteFileContent struct {
	Path string
}

type MessageDeleteFileResponse struct {
	Success bool
	Error   string
}

type MessageFileExists struct {
	Path string
}

type MessageFileExistsResponse struct {
	Exists bool
	Error  string
}

type MessageTouchFile struct {
	Path string
	Mode uint32
}

type MessageTouchFileResponse struct {
	Success bool
	Error   string
}

type MessageCatFile struct {
	Path string
}

type MessageCatFileResponse struct {
	Content []byte
	Error   string
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

func (s *FileServer) handleMessage(from string, msg *Message) error {
	switch v := msg.Payload.(type) {
	case MessageStoreFile:
		return s.handleStoreFileMessage(from, v)
	case MessageGetFile:
		return s.handleGetFileMessage(from, v)
	case MessageDeleteFile:
		return s.handleDeleteFileMessage(from, v)
	case MessageOpenFile:
		return s.handleOpenFileMessage(from, v)
	case MessageReadFile:
		return s.handleReadFileMessage(from, v)
	case MessageWriteFile:
		return s.handleWriteFileMessage(from, v)
	case MessageCloseFile:
		return s.handleCloseFileMessage(from, v)
	case MessagePutFile:
		return s.handlePutFileMessage(from, v)
	case MessageGetFileContent:
		return s.handleGetFileContentMessage(from, v)
	case MessageDeleteFileContent:
		return s.handleDeleteFileContentMessage(from, v)
	case MessageFileExists:
		return s.handleFileExistsMessage(from, v)
	case MessageTouchFile:
		return s.handleTouchFileMessage(from, v)
	case MessageCatFile:
		return s.handleCatFileMessage(from, v)
	case MessageMkdir:
		if err := s.pathStore.CreateDirRecursive(v.Path, 0755, false); err != nil {
			return err
		}
		// Don't broadcast again if this is a relayed message
		if from != "" { // If from is empty, this is a local operation
			return nil
		}
		// Broadcast to all peers except the sender
		for addr, peer := range s.peers {
			if addr == from {
				continue
			}
			peer.Send([]byte{peertopeer.IncomingMessage})
			buf := new(bytes.Buffer)
			if err := gob.NewEncoder(buf).Encode(msg); err != nil {
				return err
			}
			if err := peer.Send(buf.Bytes()); err != nil {
				return err
			}
		}
		return nil
	}

	return nil
}

func (s *FileServer) handleTouchFileMessage(from string, msg MessageTouchFile) error {
	log.Printf("[%s] Received TouchFile request for %s", s.Transport.Addr(), msg.Path)

	// Create an empty file
	err := s.StoreFile(msg.Path, strings.NewReader(""), os.FileMode(msg.Mode))

	// Send response
	response := MessageTouchFileResponse{
		Success: err == nil,
	}
	if err != nil {
		response.Error = err.Error()
	}

	return s.sendResponse(from, response)
}

func (s *FileServer) handleCatFileMessage(from string, msg MessageCatFile) error {
	log.Printf("[%s] Received CatFile request for %s", s.Transport.Addr(), msg.Path)

	// Get the file content
	reader, err := s.GetFile(msg.Path)
	if err != nil {
		response := MessageCatFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Read the file content
	content, err := io.ReadAll(reader)
	if err != nil {
		response := MessageCatFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Send the response
	response := MessageCatFileResponse{
		Content: content,
	}
	return s.sendResponse(from, response)
}

func (s *FileServer) handleDeleteFileContentMessage(from string, msg MessageDeleteFileContent) error {
	log.Printf("[%s] Received DeleteFileContent request for %s", s.Transport.Addr(), msg.Path)

	// Delete the file
	err := s.DeleteFile(msg.Path)

	// Send response
	response := MessageDeleteFileResponse{
		Success: err == nil,
	}
	if err != nil {
		response.Error = err.Error()
	}

	return s.sendResponse(from, response)
}

func (s *FileServer) handleFileExistsMessage(from string, msg MessageFileExists) error {
	log.Printf("[%s] Received FileExists request for %s", s.Transport.Addr(), msg.Path)

	// Check if the file exists
	exists := s.FileExists(msg.Path)

	// Send response
	response := MessageFileExistsResponse{
		Exists: exists,
	}

	return s.sendResponse(from, response)
}

func (s *FileServer) handlePutFileMessage(from string, msg MessagePutFile) error {
	log.Printf("[%s] Received PutFile request for %s", s.Transport.Addr(), msg.Path)

	// Store the file using the existing StoreFile method
	err := s.StoreFile(msg.Path, bytes.NewReader(msg.Content), os.FileMode(msg.Mode))
	if err != nil {
		log.Printf("[%s] Error storing file: %v", s.Transport.Addr(), err)
	}
	return err
}

func (s *FileServer) handleGetFileContentMessage(from string, msg MessageGetFileContent) error {
	log.Printf("[%s] Received GetFileContent request for %s", s.Transport.Addr(), msg.Path)

	// Get the file metadata
	meta, err := s.pathStore.Get(msg.Path)
	if err != nil {
		// Send error response
		response := MessageGetFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Get the file content
	reader, err := s.GetFile(msg.Path)
	if err != nil {
		response := MessageGetFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Read the file content
	content, err := io.ReadAll(reader)
	if err != nil {
		response := MessageGetFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Send the response
	response := MessageGetFileResponse{
		Content: content,
		Mode:    uint32(meta.Mode),
	}
	return s.sendResponse(from, response)
}

func (s *FileServer) sendResponse(to string, response interface{}) error {
	peer, ok := s.peers[to]
	if !ok {
		return fmt.Errorf("peer not found in peer map")
	}

	return gob.NewEncoder(peer).Encode(response)
}

func (s *FileServer) handleCloseFileMessage(from string, msg MessageCloseFile) error {
	log.Printf("[%s] Received CloseFile request for %s", s.Transport.Addr(), msg.Path)
	return nil // No-op for now, as we don't track open files per-peer
}

func (s *FileServer) handleWriteFileMessage(from string, msg MessageWriteFile) error {
	log.Printf("[%s] Received WriteFile request for %s at offset %d with %d bytes",
		s.Transport.Addr(), msg.Path, msg.Offset, len(msg.Data))

	file, err := s.OpenFile(msg.Path, O_WRONLY|O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Seek to the offset
	if _, err := file.Seek(msg.Offset, io.SeekStart); err != nil {
		return err
	}

	// Write data
	_, err = file.Write(msg.Data)
	return err
}

func (s *FileServer) handleReadFileMessage(from string, msg MessageReadFile) error {
	log.Printf("[%s] Received ReadFile request for %s at offset %d", s.Transport.Addr(), msg.Path, msg.Offset)

	file, err := s.Open(msg.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Seek to the offset
	if _, err := file.Seek(msg.Offset, io.SeekStart); err != nil {
		return err
	}

	// Read data
	data := make([]byte, msg.Length)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return err
	}

	// Send data back to the peer
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer not found in peer map")
	}

	peer.Send([]byte{peertopeer.IncomingStream})
	peer.Send(data[:n])
	peer.CloseStream()

	return nil
}

func (s *FileServer) handleOpenFileMessage(from string, msg MessageOpenFile) error {
	log.Printf("[%s] Received OpenFile request for %s with flags %d", s.Transport.Addr(), msg.Path, msg.Flags)
	_, err := s.OpenFile(msg.Path, msg.Flags, os.FileMode(msg.Mode))
	return err
}

func (s *FileServer) handleDeleteFileMessage(from string, msg MessageDeleteFile) error {
	meta, err := s.pathStore.Get(msg.Path)
	if err != nil {
		return err
	}

	if err := s.pathStore.Delete(msg.Path); err != nil {
		return err
	}

	return s.store.Delete(meta.ContentKey)
}

func (s *FileServer) handleGetFileMessage(from string, msg MessageGetFile) error {
	if !s.store.Has(msg.Key) {
		return fmt.Errorf("[%s]: Key not found in local store", s.Transport.Addr())
	}

	fmt.Printf("DEBUG [%s]: serving file (%s) over the network\n", s.Transport.Addr(), msg.Key)

	fileSize, r, err := s.store.Read(msg.Key)
	if err != nil {
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		defer rc.Close()
	}

	// Print the contents of the file
	contentBuf := new(bytes.Buffer)
	if _, err := io.Copy(contentBuf, r); err != nil {
		return err
	}
	fmt.Printf("DEBUG [%s]: File contents: %s\n", s.Transport.Addr(), contentBuf.String())

	// Reset the reader to serve the file over the network
	r = bytes.NewReader(contentBuf.Bytes())

	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer %s found in peer map", from)
	}

	peer.Send([]byte{peertopeer.IncomingStream})
	binary.Write(peer, binary.LittleEndian, fileSize)
	n, err := io.Copy(peer, r)
	if err != nil {
		return err
	}

	log.Printf("[%s]: Sent %d bytes to %s\n", s.Transport.Addr(), n, peer.RemoteAddr())
	return nil
}

func (s *FileServer) handleStoreFileMessage(from string, msg MessageStoreFile) error {
	peer, ok := s.peers[from]
	if !ok {
		return fmt.Errorf("peer not found in peer map")
	}

	// Store the file content
	log.Printf("[%s]: Storing file (%s) in local store\n", s.Transport.Addr(), msg.Key)
	contentBuf := new(bytes.Buffer)
	n, err := s.store.Write(msg.Key, io.TeeReader(io.LimitReader(peer, msg.Size), contentBuf))
	if err != nil {
		return err
	}

	// Print the contents of the file
	log.Printf("[%s]: File contents: %s\n", s.Transport.Addr(), contentBuf.String())

	// Store the metadata
	fileType := FileTypeRegular
	if err := s.pathStore.Set(msg.Path, msg.Key, n, os.FileMode(msg.Mode), fileType); err != nil {
		return err
	}

	log.Printf("[%s] Stored %d bytes for path %s\n", s.Transport.Addr(), n, msg.Path)
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
	gob.Register(MessageDeleteFile{})
	gob.Register(MessageOpenFile{})
	gob.Register(MessageReadFile{})
	gob.Register(MessageWriteFile{})
	gob.Register(MessageCloseFile{})
	gob.Register(MessageMkdir{})
	gob.Register(MessagePutFile{})
	gob.Register(MessageGetFileContent{})
	gob.Register(MessageGetFileResponse{})
	gob.Register(MessageDeleteFileContent{})
	gob.Register(MessageDeleteFileResponse{})
	gob.Register(MessageFileExists{})
	gob.Register(MessageFileExistsResponse{})
	gob.Register(MessageTouchFile{})
	gob.Register(MessageTouchFileResponse{})
	gob.Register(MessageCatFile{})
	gob.Register(MessageCatFileResponse{})
}
