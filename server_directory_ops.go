package main

import (
	"log"
	"os"
)

// MakeDirectory creates a directory and broadcasts the operation to peers
func (s *FileServer) MakeDirectory(path string, mode os.FileMode, recursive bool) error {
	// Create the directory locally
	var err error
	if recursive {
		err = s.pathStore.CreateDirRecursive(path, mode, false)
	} else {
		err = s.pathStore.CreateDir(path, mode, false)
	}

	if err != nil {
		return err
	}

	// Broadcast to peers
	msg := Message{
		Payload: MessageMkdir{
			Path:      path,
			Recursive: recursive,
		},
	}

	if err := s.broadcast(&msg); err != nil {
		log.Printf("[%s] Error broadcasting directory creation: %v", s.Transport.Addr(), err)
	}

	return nil
}