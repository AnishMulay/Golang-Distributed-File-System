package main

import (
	"log"
	"net"
	"os"
)

func handleFileCreated(conn net.Conn, filePath string) {
	log.Println("Handling file created event for file:", filePath)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	// Get the file info
	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("Failed to get file info for %s: %v", filePath, err)
		return
	}

	log.Printf("File size: %d bytes", fileInfo.Size())
}
