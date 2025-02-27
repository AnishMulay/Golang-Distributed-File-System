package main

import (
	"log"

	"github.com/AnishMulay/Golang-Distributed-File-System/peertopeer"
)

func main() {
	tr := peertopeer.NewTCPTransport(":3000")
	if err := tr.ListenAndAccept(); err != nil {
		log.Fatal(err)
	}

	select {}
}
