package main

import (
	"log"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

type FileServerConfig struct {
	StorageRoot       string
	PathTransformFunc PathTransformFunc
	Transport         peertopeer.Transport
}

type FileServer struct {
	FileServerConfig
	store  *Store
	quitch chan struct{}
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
	}
}

func (s *FileServer) Stop() {
	close(s.quitch)
}

func (s *FileServer) loop() {
	for {
		select {
		case msg := <-s.Transport.Consume():
			log.Println("Received message", msg)
		case <-s.quitch:
			return
		}
	}
}

func (s *FileServer) Start() error {
	if err := s.Transport.ListenAndAccept(); err != nil {
		return err
	}

	s.loop()

	return nil
}
