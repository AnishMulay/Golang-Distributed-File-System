package main

import (
	"log"
	"os"
)

// CreateDirectory creates a directory at the given path and broadcasts to peers
func (s *FileServer) CreateDirectory(path string, mode os.FileMode, recursive bool) error {
	var err error
	if recursive {
		err = s.pathStore.CreateDirRecursive(path, mode, false)
	} else {
		err = s.pathStore.CreateDir(path, mode, false)
	}

	if err != nil {
		return err
	}

	// Broadcast directory creation to peers
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