package main

import (
	"ashishkujoy/queue/internal/config"
	netinternal "ashishkujoy/queue/internal/net"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	conf := config.NewConfigWithMetadataPath("data/segments", "data/metadata", 1024*1024)
	server, err := netinternal.NewQueueServer(conf, ":8081")
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
		return
	}
	serverStop := make(chan interface{})
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		println("About to call run")
		server.Run(serverStop)
		println("Calling run done")
	}()
	<-stop
	serverStop <- "stop"
}
