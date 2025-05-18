package main

import (
	"ashishkujoy/queue/internal/config"
	netinternal "ashishkujoy/queue/internal/net"
	"log"
)

func main() {
	conf := config.NewConfigWithMetadataPath("data/segments", "data/metadata", 1024*1024)
	server, err := netinternal.NewQueueServer(conf, ":50051")
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
		return
	}

	err = server.Run()
}
