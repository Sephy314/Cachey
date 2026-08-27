package main

import (
	"log"
	"os"

	"github.com/Sephy314/Cachey/internal/protocol"
	"github.com/Sephy314/Cachey/pkg/client"
)

func main() {
	address := "localhost:8080"
	if len(os.Args) > 1 {
		address = os.Args[1]
	}

	client, err := client.NewClient(address)
	if err != nil {
		log.Println("Error creating client:", err.Error())
		return
	}
	defer client.Close()

	log.Println("Client connected to server at", address)

	// Test PUT command
	putCmd := protocol.Command{Type: protocol.PUT, Key: "testKey", Val: "testValue"}
	response, err := client.SendCommand(putCmd)
	if err != nil {
		log.Println("Error sending PUT command:", err.Error())
		return
	}
	log.Println("PUT response:", *response)

	// Test GET command
	getCmd := protocol.Command{Type: protocol.GET, Key: "testKey"}
	response, err = client.SendCommand(getCmd)
	if err != nil {
		log.Println("Error sending GET command:", err.Error())
		return
	}
	log.Println("GET response:", *response)

	// Test DELETE command
	deleteCmd := protocol.Command{Type: protocol.DEL, Key: "testKey"}
	response, err = client.SendCommand(deleteCmd)
	if err != nil {
		log.Println("Error sending DELETE command:", err.Error())
		return
	}
	log.Println("DELETE response:", *response)

}
