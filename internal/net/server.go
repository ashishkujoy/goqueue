package netinternal

import (
	"ashishkujoy/queue/internal"
	"ashishkujoy/queue/internal/config"
	queueinternal "ashishkujoy/queue/internal/queue"
	netinternal "ashishkujoy/queue/proto"
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MessageOutputStream = grpc.ServerStreamingServer[netinternal.QueueMessage]
type OnlineConsumer struct {
	id           uint64
	stream       MessageOutputStream
	closeChannel chan<- interface{}
}
type QueueServer struct {
	netinternal.UnimplementedQueueServiceServer
	queueService   *queueinternal.QueueService
	port           string
	gpServer       *grpc.Server
	onlineConsumer []*OnlineConsumer
	mu             *sync.RWMutex
}

func NewQueueServer(config *config.Config, port string) (*QueueServer, error) {
	service, err := queueinternal.NewQueueService(config)
	if err != nil {
		return nil, err
	}

	gpServer := grpc.NewServer()
	server := &QueueServer{
		port:           port,
		queueService:   service,
		gpServer:       gpServer,
		onlineConsumer: make([]*OnlineConsumer, 0),
		mu:             &sync.RWMutex{},
	}
	netinternal.RegisterQueueServiceServer(gpServer, server)
	return server, nil
}

func (qs *QueueServer) Enqueue(_ context.Context, req *netinternal.EnqueueRequest) (*netinternal.EnqueueRequestResponse, error) {
	if err := qs.queueService.Enqueue(req.Message); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to enqueue")
	}
	go qs.broadcastMessage()
	return &netinternal.EnqueueRequestResponse{Success: true}, nil
}

func (qs *QueueServer) broadcastMessage() {
	var closedChannels []uint64
	mu := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, consumer := range qs.onlineConsumer {
		wg.Add(1)
		go func(consumer *OnlineConsumer) {
			defer wg.Done()
			if err := qs.serveMessages(consumer); err != nil {
				mu.Lock()
				defer mu.Unlock()
				qs.queueService.RevertDequeue(int(consumer.id))
				consumer.closeChannel <- "closed"
				closedChannels = append(closedChannels, consumer.id)
			}
		}(consumer)
	}
	wg.Wait()
	qs.onlineConsumer = internal.Filter(qs.onlineConsumer, func(consumer *OnlineConsumer) bool {
		return !internal.Contains(closedChannels, consumer.id)
	})
}

func (qs *QueueServer) ObserveQueue(req *netinternal.ObserveQueueRequest, stream grpc.ServerStreamingServer[netinternal.QueueMessage]) error {
	closeChannel := make(chan interface{})
	consumer := &OnlineConsumer{id: req.ConsumerId, stream: stream, closeChannel: closeChannel}
	_ = qs.serveMessages(consumer)
	qs.onlineConsumer = append(qs.onlineConsumer, consumer)
	<-closeChannel
	return nil
}

func (qs *QueueServer) serveMessages(consumer *OnlineConsumer) error {
	for {
		msg, err := qs.queueService.Dequeue(int(consumer.id))
		if err != nil {
			break
		}
		err = consumer.stream.Send(&netinternal.QueueMessage{Message: msg})
		if err != nil {
			return err
		}
	}

	return nil
}

func (qs *QueueServer) Run() error {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Printf("Error Creating listener %v\n", err)
		return err
	}
	fmt.Println("Created Listener")
	if err := qs.gpServer.Serve(listener); err != nil {
		return nil
	}
	return nil
}
