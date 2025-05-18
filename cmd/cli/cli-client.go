package main

import (
	netinternal "ashishkujoy/queue/proto"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"log"
	"time"
)

type CLIOptions struct {
	msg        string
	publish    bool
	consumerId uint64
}

func NewCLIOptions() *CLIOptions {
	msg := flag.String("msg", "", "message to send")
	publish := flag.Bool("publish", false, "publish message to the queue")
	consumerId := flag.Uint64("consumer-id", 0, "consumer id")

	flag.Parse()

	return &CLIOptions{
		msg:        *msg,
		publish:    *publish,
		consumerId: *consumerId,
	}
}

func createQueueClient() netinternal.QueueServiceClient {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	client := netinternal.NewQueueServiceClient(conn)
	return client
}

func enqueueMsg(cliOptions *CLIOptions, client netinternal.QueueServiceClient) {
	fmt.Printf("publishing message to queue: %s\n", cliOptions.msg)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.Enqueue(ctx, &netinternal.EnqueueRequest{Message: []byte(cliOptions.msg)})
	if err != nil {
		log.Fatalf("failed to enqueue: %v", err)
	}
	return
}

func observeQueueMsg(cliOptions *CLIOptions, client netinternal.QueueServiceClient) {
	fmt.Printf("Observing message from queue, consumer id = %d\n", cliOptions.consumerId)
	queue, err := client.ObserveQueue(context.Background(), &netinternal.ObserveQueueRequest{ConsumerId: cliOptions.consumerId})
	if err != nil {
		log.Fatalf("failed to observe: %v", err)
	}
	for {
		queueMessage, err := queue.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("failed to receive: %v", err)
		}
		fmt.Printf("received message: %s\n", string(queueMessage.Message))
	}
}

func main() {
	cliOptions := NewCLIOptions()
	client := createQueueClient()

	if cliOptions.publish {
		enqueueMsg(cliOptions, client)
		return
	}

	observeQueueMsg(cliOptions, client)
}
