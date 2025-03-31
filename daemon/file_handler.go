package main

import (
	"log"
	"net"
)

// func handleFileCreated(filePath string) {
// 	log.Println("Handling file created event for file:", filePath)
// }

func handleFileCreated(conn net.Conn, filePath string) {
	log.Println("Handling file created event for file:", filePath)
}
