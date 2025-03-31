package main

import (
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
)

func main() {
	// Establish a TCP connection to the server on localhost:3000
	conn, err := net.Dial("tcp", "localhost:3000")
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	log.Println("Connected to server on localhost:3000")

	// Set up the file watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	defer watcher.Close()

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				log.Println("event:", event)
				if event.Has(fsnotify.Create) {
					handleFileCreated(conn, event.Name)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("error:", err)
			}
		}
	}()

	// Get the current working directory
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	// Create the path to the temp directory
	tempDirPath := filepath.Join(currentDir, "temp")
	log.Println("Listening for changes in:", tempDirPath)

	err = watcher.Add(tempDirPath)
	if err != nil {
		log.Fatal(err)
	}

	<-make(chan struct{})
}
