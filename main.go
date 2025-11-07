package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/pjmessi/go_file_share/internal/receiver"
	"github.com/pjmessi/go_file_share/internal/sender"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "", "port number")
	flag.Parse()

	fmt.Println("Press 's' to send files and 'r' to receive files")
	var purpose string
	fmt.Scanln(&purpose)

	udpDiscoveryPort := uint(9999)
	chunkSize := uint(1024)
	receiver := receiver.NewReceiver(chunkSize, udpDiscoveryPort)
	sender := sender.NewSender(chunkSize, udpDiscoveryPort)

	switch purpose {
	case "s":
		if err := sender.Handle(port); err != nil {
			log.Fatalf("err starting sender: %s", err)
		}

	case "r":
		if err := receiver.Handle(); err != nil {
			log.Fatalf("err receiving file from the sender: %s", err)
		}

	default:
		log.Println("invalid input")
	}
}
