package main

import (
	"fmt"
	"log"
	"os"
)

var version = "0.1.0"

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		fmt.Println("No arguments provided")
		os.Exit(1)
	}

	cmd := args[0]

	switch cmd {
	case "version":
		fmt.Println(version)
		os.Exit(0)
	case "run":
		log.Println("Simod job controller has started")
	default:
		fmt.Printf("Unknown command: %s", cmd)
		os.Exit(1)
	}

	run()
}

func run() {
	log.Println("Simod job controller is running")
}
