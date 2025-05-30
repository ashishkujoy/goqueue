package main

import (
	"ashishkujoy/queue/internal/config"
	netinternal "ashishkujoy/queue/internal/net"
	"log"
	"time"
)

func main() {
	conf := config.NewConfig(
		"data/segments",
		"data/metadata",
		1024*1024,
		time.Second*2,
	)
	server, err := netinternal.NewQueueServer(conf, ":50051")
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
		return
	}

	err = server.Run()
	if err != nil {
		log.Fatalf("Server run failed: %v", err)
	}
}
