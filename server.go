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

        "github.com/AnishMulay/Golang-Distributed-File-System/config"
        "github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

type FileServerConfig struct {
        EncryptionKey     []byte
        StorageRoot       string
        MetadataRoot      string
        PathTransformFunc PathTransformFunc
        Transport         peertopeer.Transport
        BootstrapPeers    []string
        // Node configuration
        Mode             config.NodeMode
        Role             config.NodeRole
        NodeID           string
        LeaderNodes      []string
        // Replication settings
        ReplicationFactor int
        AsyncReplication  bool
        AsyncQueueSize    int
        ConsistencyLevel  string
        // Storage settings
        DefaultFileMode uint32
        MaxFileSize     int64
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

        pathStore, err := NewPathStore(config.MetadataRoot)
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
        s.peerLock.Lock()
        defer s.peerLock.Unlock()
        
        log.Printf("[DEBUG BROADCAST %s]: Broadcasting message of type %T to %d peers", 
                s.Transport.Addr(), msg.Payload, len(s.peers))
        
        if len(s.peers) == 0 {
                log.Printf("[DEBUG BROADCAST %s]: No peers to broadcast to", s.Transport.Addr())
                return nil
        }
        
        buf := new(bytes.Buffer)
        if err := gob.NewEncoder(buf).Encode(msg); err != nil {
                log.Printf("[DEBUG BROADCAST %s]: Error encoding message: %v", s.Transport.Addr(), err)
                return err
        }
        
        msgBytes := buf.Bytes()
        
        for addr, peer := range s.peers {
                log.Printf("[DEBUG BROADCAST %s]: Sending message to peer %s", s.Transport.Addr(), addr)
                
                // Send the message type indicator first
                if err := peer.Send([]byte{peertopeer.IncomingMessage}); err != nil {
                        log.Printf("[DEBUG BROADCAST %s]: Error sending message type to %s: %v", 
                                s.Transport.Addr(), addr, err)
                        continue
                }
                
                // Then send the actual message
                if err := peer.Send(msgBytes); err != nil {
                        log.Printf("[DEBUG BROADCAST %s]: Error sending message to %s: %v", 
                                s.Transport.Addr(), addr, err)
                        continue
                }
                
                log.Printf("[DEBUG BROADCAST %s]: Successfully sent message to %s", s.Transport.Addr(), addr)
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
        log.Printf("[DEBUG GETFILE %s]: Getting file for path %s", s.Transport.Addr(), path)

        meta, err := s.pathStore.Get(path)
        if err != nil {
                log.Printf("[DEBUG GETFILE %s]: Error getting metadata for path %s: %v", 
                        s.Transport.Addr(), path, err)
                return nil, err
        }
        
        log.Printf("[DEBUG GETFILE %s]: Found metadata for path %s: contentKey=%s, size=%d", 
                s.Transport.Addr(), path, meta.ContentKey, meta.Size)

        // Update access time
        meta.AccessTime = time.Now()
        s.pathStore.Set(path, meta.ContentKey, meta.Size, meta.Mode, meta.Type)

        // Get the file content using the content key
        log.Printf("[DEBUG GETFILE %s]: Calling Get() for contentKey %s", 
                s.Transport.Addr(), meta.ContentKey)
        reader, err := s.Get(meta.ContentKey)
        if err != nil {
                log.Printf("[DEBUG GETFILE %s]: Error in Get() for contentKey %s: %v", 
                        s.Transport.Addr(), meta.ContentKey, err)
                return nil, err
        }
        
        log.Printf("[DEBUG GETFILE %s]: Successfully retrieved reader for contentKey %s", 
                s.Transport.Addr(), meta.ContentKey)
        return reader, nil
}

// StoreFile stores a file at the given path
func (s *FileServer) StoreFile(path string, r io.Reader, mode os.FileMode) error {
        path = filepath.Clean(path)
        log.Printf("[DEBUG STOREFILE %s]: Storing file at path %s", s.Transport.Addr(), path)

        // Create a buffer to read the entire file
        buf := new(bytes.Buffer)
        tee := io.TeeReader(r, buf)

        // Generate a content key based on the path
        // In a real implementation, this would be a hash of the content
        contentKey := path

        // Store in the content-addressable store
        log.Printf("[DEBUG STOREFILE %s]: Storing file (%s) in local store", s.Transport.Addr(), contentKey)
        size, err := s.store.Write(contentKey, tee)
        if err != nil {
                log.Printf("[DEBUG STOREFILE %s]: Error writing to store: %v", s.Transport.Addr(), err)
                return err
        }

        // Store metadata
        fileType := FileTypeRegular
        if err := s.pathStore.Set(path, contentKey, size, mode, fileType); err != nil {
                log.Printf("[DEBUG STOREFILE %s]: Error setting metadata: %v", s.Transport.Addr(), err)
                return err
        }

        // Lock the peer map before accessing it
        s.peerLock.Lock()
        peerCount := len(s.peers)
        s.peerLock.Unlock()
        
        if peerCount == 0 {
                log.Printf("[DEBUG STOREFILE %s]: No peers to broadcast to", s.Transport.Addr())
                return nil
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

        log.Printf("[DEBUG STOREFILE %s]: Broadcasting to %d peers", s.Transport.Addr(), peerCount)
        if err := s.broadcast(&msg); err != nil {
                log.Printf("[DEBUG STOREFILE %s]: Error broadcasting message: %v", s.Transport.Addr(), err)
                return err
        }

        // Wait to ensure the message is processed before sending the stream
        time.Sleep(100 * time.Millisecond)

        // Lock the peer map again before sending the file content
        s.peerLock.Lock()
        peers := []io.Writer{}
        peerAddrs := []string{}
        
        for addr, peer := range s.peers {
                log.Printf("[DEBUG STOREFILE %s]: Adding peer %s to multiwriter", s.Transport.Addr(), addr)
                peers = append(peers, peer)
                peerAddrs = append(peerAddrs, addr)
        }
        s.peerLock.Unlock()

        if len(peers) == 0 {
                log.Printf("[DEBUG STOREFILE %s]: No peers to send file to", s.Transport.Addr())
                return nil
        }

        // Send the file content to each peer individually to avoid issues with MultiWriter
        for i, peer := range peers {
                addr := peerAddrs[i]
                log.Printf("[DEBUG STOREFILE %s]: Sending file content to peer %s", s.Transport.Addr(), addr)
                
                // Send stream indicator
                peer.Write([]byte{peertopeer.IncomingStream})
                
                // Reset buffer position
                bufCopy := bytes.NewBuffer(buf.Bytes())
                
                // Send encrypted data
                n, err := copyEncrypt(s.EncryptionKey, bufCopy, peer)
                if err != nil {
                        log.Printf("[DEBUG STOREFILE %s]: Error sending to peer %s: %v", s.Transport.Addr(), addr, err)
                        continue
                }
                
                log.Printf("[DEBUG STOREFILE %s]: Sent %d bytes to peer %s", s.Transport.Addr(), n, addr)
                
                // Give peer time to process before closing stream
                time.Sleep(50 * time.Millisecond)
                
                // Get the peer again in case the map has changed
                s.peerLock.Lock()
                if p, ok := s.peers[addr]; ok {
                        log.Printf("[DEBUG STOREFILE %s]: Closing stream on peer %s", s.Transport.Addr(), addr)
                        p.CloseStream()
                }
                s.peerLock.Unlock()
        }
        
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