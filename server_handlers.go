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

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

// handleMessage routes incoming messages to the appropriate handler based on message type
func (s *FileServer) handleMessage(from string, msg *Message) error {
	// Log the message type for debugging
	log.Printf("[%s] Handling message of type %T from %s", s.Transport.Addr(), msg.Payload, from)
	
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
	case MessageLsDirectory:
		return s.handleLsDirectoryMessage(from, v)
	case MessageMkdir:
		return s.handleMkdirMessage(from, v)
	default:
		return fmt.Errorf("unknown message type: %T", v)
	}
}

// sendResponse sends a response to a peer
func (s *FileServer) sendResponse(to string, response interface{}) error {
	// Make sure all types are registered
	RegisterMessageTypes()
	
	s.peerLock.Lock()
	peer, ok := s.peers[to]
	s.peerLock.Unlock()
	
	if !ok {
		log.Printf("[DEBUG RESPONSE %s]: Peer %s not found in peer map", s.Transport.Addr(), to)
		return fmt.Errorf("peer not found in peer map")
	}

	log.Printf("[DEBUG RESPONSE %s]: Sending response of type %T to %s", 
		s.Transport.Addr(), response, to)
	
	// Create a message wrapper for the response
	msg := Message{
		Payload: response,
	}
	
	// Encode the message
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(msg); err != nil {
		log.Printf("[DEBUG RESPONSE %s]: Error encoding response: %v", s.Transport.Addr(), err)
		return err
	}
	
	// Send the message
	if err := peer.Send([]byte{peertopeer.IncomingMessage}); err != nil {
		log.Printf("[DEBUG RESPONSE %s]: Error sending message type: %v", s.Transport.Addr(), err)
		return err
	}
	
	if err := peer.Send(buf.Bytes()); err != nil {
		log.Printf("[DEBUG RESPONSE %s]: Error sending response data: %v", s.Transport.Addr(), err)
		return err
	}
	
	log.Printf("[DEBUG RESPONSE %s]: Successfully sent response to %s", s.Transport.Addr(), to)
	return nil
}

// File operation handlers
func (s *FileServer) handleStoreFileMessage(from string, msg MessageStoreFile) error {
	log.Printf("[DEBUG STORE %s]: Received StoreFile message from %s for key %s, path %s, size %d", 
		s.Transport.Addr(), from, msg.Key, msg.Path, msg.Size)
	
	peer, ok := s.peers[from]
	if !ok {
		log.Printf("[DEBUG STORE %s]: Peer %s not found in peer map", s.Transport.Addr(), from)
		return fmt.Errorf("peer not found in peer map")
	}

	// First, store the metadata to ensure it's saved even if content streaming fails
	fileType := FileTypeRegular
	if err := s.pathStore.Set(msg.Path, msg.Key, 0, os.FileMode(msg.Mode), fileType); err != nil {
		log.Printf("[DEBUG STORE %s]: Error setting initial metadata: %v", s.Transport.Addr(), err)
		return err
	}
	log.Printf("[DEBUG STORE %s]: Stored initial metadata for path %s", s.Transport.Addr(), msg.Path)

	// Now read the file content
	log.Printf("[DEBUG STORE %s]: Waiting for file content stream from %s", s.Transport.Addr(), from)
	
	contentBuf := new(bytes.Buffer)
	limitReader := io.LimitReader(peer, msg.Size)
	teeReader := io.TeeReader(limitReader, contentBuf)
	
	n, err := s.store.Write(msg.Key, teeReader)
	if err != nil {
		log.Printf("[DEBUG STORE %s]: Error writing to store: %v", s.Transport.Addr(), err)
		return err
	}

	// Print the contents of the file
	log.Printf("[DEBUG STORE %s]: File contents: %s", s.Transport.Addr(), contentBuf.String())

	// Update the metadata with the correct size
	if err := s.pathStore.Set(msg.Path, msg.Key, n, os.FileMode(msg.Mode), fileType); err != nil {
		log.Printf("[DEBUG STORE %s]: Error updating metadata with size: %v", s.Transport.Addr(), err)
		return err
	}

	log.Printf("[DEBUG STORE %s]: Successfully stored %d bytes for path %s", s.Transport.Addr(), n, msg.Path)
	
	// Make sure to close the stream
	peer.CloseStream()
	log.Printf("[DEBUG STORE %s]: Closed stream from %s", s.Transport.Addr(), from)
	
	return nil
}

func (s *FileServer) handleGetFileMessage(from string, msg MessageGetFile) error {
	log.Printf("[DEBUG HANDLER %s]: Received GetFile request for key %s from %s", 
		s.Transport.Addr(), msg.Key, from)
	
	if !s.store.Has(msg.Key) {
		log.Printf("[DEBUG HANDLER %s]: Key %s not found in local store", 
			s.Transport.Addr(), msg.Key)
		return fmt.Errorf("[%s]: Key not found in local store", s.Transport.Addr())
	}

	log.Printf("[DEBUG HANDLER %s]: Serving file (%s) over the network to %s", 
		s.Transport.Addr(), msg.Key, from)

	fileSize, r, err := s.store.Read(msg.Key)
	if err != nil {
		log.Printf("[DEBUG HANDLER %s]: Error reading file: %v", s.Transport.Addr(), err)
		return err
	}

	if rc, ok := r.(io.ReadCloser); ok {
		defer rc.Close()
	}

	// Print the contents of the file
	contentBuf := new(bytes.Buffer)
	if _, err := io.Copy(contentBuf, r); err != nil {
		log.Printf("[DEBUG HANDLER %s]: Error copying file contents: %v", s.Transport.Addr(), err)
		return err
	}
	log.Printf("[DEBUG HANDLER %s]: File contents: %s", s.Transport.Addr(), contentBuf.String())

	// Reset the reader to serve the file over the network
	r = bytes.NewReader(contentBuf.Bytes())

	peer, ok := s.peers[from]
	if !ok {
		log.Printf("[DEBUG HANDLER %s]: Peer %s not found in peer map", s.Transport.Addr(), from)
		return fmt.Errorf("peer %s not found in peer map", from)
	}

	log.Printf("[DEBUG HANDLER %s]: Sending IncomingStream signal to peer %s", 
		s.Transport.Addr(), from)
	peer.Send([]byte{peertopeer.IncomingStream})
	
	log.Printf("[DEBUG HANDLER %s]: Writing file size %d to peer %s", 
		s.Transport.Addr(), fileSize, from)
	err = binary.Write(peer, binary.LittleEndian, fileSize)
	if err != nil {
		log.Printf("[DEBUG HANDLER %s]: Error writing file size: %v", s.Transport.Addr(), err)
		return err
	}
	
	log.Printf("[DEBUG HANDLER %s]: Copying file data to peer %s", s.Transport.Addr(), from)
	n, err := io.Copy(peer, r)
	if err != nil {
		log.Printf("[DEBUG HANDLER %s]: Error copying file data: %v", s.Transport.Addr(), err)
		return err
	}

	log.Printf("[DEBUG HANDLER %s]: Successfully sent %d bytes to %s", 
		s.Transport.Addr(), n, peer.RemoteAddr())
	return nil
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

// File access handlers
func (s *FileServer) handleOpenFileMessage(from string, msg MessageOpenFile) error {
	log.Printf("[%s] Received OpenFile request for %s with flags %d", s.Transport.Addr(), msg.Path, msg.Flags)
	_, err := s.OpenFile(msg.Path, msg.Flags, os.FileMode(msg.Mode))
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

func (s *FileServer) handleCloseFileMessage(from string, msg MessageCloseFile) error {
	log.Printf("[%s] Received CloseFile request for %s", s.Transport.Addr(), msg.Path)
	return nil // No-op for now, as we don't track open files per-peer
}

// Client command handlers
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
	log.Printf("[DEBUG CLIENT %s]: Received GetFileContent request for %s from %s", 
		s.Transport.Addr(), msg.Path, from)

	// Get the file metadata
	meta, err := s.pathStore.Get(msg.Path)
	if err != nil {
		log.Printf("[DEBUG CLIENT %s]: Error getting metadata for %s: %v", 
			s.Transport.Addr(), msg.Path, err)
		// Send error response
		response := MessageGetFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}
	
	log.Printf("[DEBUG CLIENT %s]: Found metadata for %s: contentKey=%s, size=%d", 
		s.Transport.Addr(), msg.Path, meta.ContentKey, meta.Size)

	// Check if content exists in local store
	if !s.store.Has(meta.ContentKey) {
		log.Printf("[DEBUG CLIENT %s]: Content key %s not found in local store", 
			s.Transport.Addr(), meta.ContentKey)
		response := MessageGetFileResponse{
			Error: fmt.Sprintf("content for %s not available", msg.Path),
		}
		return s.sendResponse(from, response)
	}

	// Read directly from store instead of using GetFile
	_, reader, err := s.store.Read(meta.ContentKey)
	if err != nil {
		log.Printf("[DEBUG CLIENT %s]: Error reading from store: %v", s.Transport.Addr(), err)
		response := MessageGetFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Read the file content
	log.Printf("[DEBUG CLIENT %s]: Reading all content for %s", s.Transport.Addr(), msg.Path)
	content, err := io.ReadAll(reader)
	if err != nil {
		log.Printf("[DEBUG CLIENT %s]: Error reading file content for %s: %v", 
			s.Transport.Addr(), msg.Path, err)
		response := MessageGetFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}
	
	log.Printf("[DEBUG CLIENT %s]: Successfully read %d bytes for %s", 
		s.Transport.Addr(), len(content), msg.Path)

	// Send the response
	log.Printf("[DEBUG CLIENT %s]: Sending response with %d bytes for %s", 
		s.Transport.Addr(), len(content), msg.Path)
	response := MessageGetFileResponse{
		Content: content,
		Mode:    uint32(meta.Mode),
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
	log.Printf("[DEBUG CAT %s] Received CatFile request for %s from %s", 
		s.Transport.Addr(), msg.Path, from)

	// Check if file exists locally first
	if !s.FileExists(msg.Path) {
		log.Printf("[DEBUG CAT %s] File %s does not exist", s.Transport.Addr(), msg.Path)
		response := MessageCatFileResponse{
			Error: fmt.Sprintf("file %s not found", msg.Path),
		}
		return s.sendResponse(from, response)
	}

	// Get metadata directly
	meta, err := s.pathStore.Get(msg.Path)
	if err != nil {
		log.Printf("[DEBUG CAT %s] Error getting metadata: %v", s.Transport.Addr(), err)
		response := MessageCatFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}
	
	log.Printf("[DEBUG CAT %s] Found metadata for %s: contentKey=%s", 
		s.Transport.Addr(), msg.Path, meta.ContentKey)

	// Check if content exists in local store
	if !s.store.Has(meta.ContentKey) {
		log.Printf("[DEBUG CAT %s] Content key %s not found in local store", 
			s.Transport.Addr(), meta.ContentKey)
		response := MessageCatFileResponse{
			Error: fmt.Sprintf("content for %s not available", msg.Path),
		}
		return s.sendResponse(from, response)
	}

	// Read directly from store instead of using GetFile
	_, reader, err := s.store.Read(meta.ContentKey)
	if err != nil {
		log.Printf("[DEBUG CAT %s] Error reading from store: %v", s.Transport.Addr(), err)
		response := MessageCatFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	// Read the file content
	content, err := io.ReadAll(reader)
	if err != nil {
		log.Printf("[DEBUG CAT %s] Error reading content: %v", s.Transport.Addr(), err)
		response := MessageCatFileResponse{
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}

	log.Printf("[DEBUG CAT %s] Successfully read %d bytes for %s", 
		s.Transport.Addr(), len(content), msg.Path)

	// Send the response
	response := MessageCatFileResponse{
		Content: content,
	}
	log.Printf("[DEBUG CAT %s] Sending response with %d bytes", 
		s.Transport.Addr(), len(content))
	return s.sendResponse(from, response)
}

func (s *FileServer) handleLsDirectoryMessage(from string, msg MessageLsDirectory) error {
	log.Printf("[%s] Received LsDirectory request for %s", s.Transport.Addr(), msg.Path)

	// List the directory contents
	entries, err := s.pathStore.ListDir(msg.Path, nil)
	if err != nil {
		response := MessageLsDirectoryResponse{
			Files: []FileInfo{},
			Error: err.Error(),
		}
		return s.sendResponse(from, response)
	}
	
	// Convert to FileInfo structs
	files := make([]FileInfo, 0, len(entries))
	for _, entry := range entries {
		fileInfo := FileInfo{
			Name:    filepath.Base(entry.Path),
			Size:    entry.Size,
			Mode:    uint32(entry.Mode),
			IsDir:   entry.IsDir,
			ModTime: entry.ModTime.Unix(),
		}
		files = append(files, fileInfo)
	}
	
	// Send the response
	response := MessageLsDirectoryResponse{
		Files: files,
		Error: "",
	}
	
	return s.sendResponse(from, response)
}

func (s *FileServer) handleMkdirMessage(from string, msg MessageMkdir) error {
	log.Printf("[%s] Received Mkdir request for %s (recursive: %v)",
		s.Transport.Addr(), msg.Path, msg.Recursive)

	// Create the directory locally without broadcasting
	var err error
	if msg.Recursive {
		err = s.pathStore.CreateDirRecursive(msg.Path, 0755, false)
	} else {
		err = s.pathStore.CreateDir(msg.Path, 0755, false)
	}
	
	// Only broadcast if this is a client request (not from another peer)
	// and if the directory was created successfully
	if err == nil && from != "" && !strings.HasPrefix(from, "peer:") {
		// Broadcast to peers
		go func() {
			broadcastMsg := Message{
				Payload: MessageMkdir{
					Path:      msg.Path,
					Recursive: msg.Recursive,
				},
			}
			
			if broadcastErr := s.broadcast(&broadcastMsg); broadcastErr != nil {
				log.Printf("[%s] Error broadcasting mkdir: %v", s.Transport.Addr(), broadcastErr)
			}
		}()
	}
	
	// Send response
	response := MessageMkdirResponse{
		Success: err == nil,
	}
	if err != nil {
		response.Error = err.Error()
	}
	
	return s.sendResponse(from, response)
}
