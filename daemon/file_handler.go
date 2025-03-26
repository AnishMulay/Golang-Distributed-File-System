package main

import "log"

// I want a function which I can call when the daemon hears a change, and the change is that a new file has been created
// I want to be able to pass the file path to the function
// Then I want to print out the file path
// The function should be called handleFileCreated

func handleFileCreated(filePath string) {
	log.Println("Handling file created event for file:", filePath)
}
