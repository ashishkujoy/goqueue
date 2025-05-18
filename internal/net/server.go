package netinternal

import (
	"ashishkujoy/queue/internal"
	"ashishkujoy/queue/internal/config"
	queueinternal "ashishkujoy/queue/internal/queue"
	netinternal "ashishkujoy/queue/proto"
	context "context"
	"fmt"
	"log"
	"net"
	"sync"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
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
	//qs.mu.Lock()
	//defer qs.mu.Unlock()
	if err := qs.queueService.Enqueue(req.Message); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to enqueue")
	}
	qs.broadcastMessage()
	return &netinternal.EnqueueRequestResponse{Success: true}, nil
}

func (qs *QueueServer) broadcastMessage() {
	//qs.mu.RLock()
	//defer qs.mu.RUnlock()
	var closedChannels []uint64
	for _, consumer := range qs.onlineConsumer {
		if err := qs.serveMessages(consumer); err != nil {
			consumer.closeChannel <- "closed"
			closedChannels = append(closedChannels, consumer.id)
		}
	}
	qs.onlineConsumer = internal.Filter(qs.onlineConsumer, func(consumer *OnlineConsumer) bool {
		return !internal.Contains(closedChannels, consumer.id)
	})
}

func (qs *QueueServer) ObserveQueue(req *netinternal.ObserveQueueRequest, stream grpc.ServerStreamingServer[netinternal.QueueMessage]) error {
	closeChannel := make(chan interface{})
	consumer := &OnlineConsumer{id: req.ConsumerId, stream: stream, closeChannel: closeChannel}
	_ = qs.serveMessages(consumer)
	//qs.mu.Lock()
	//defer qs.mu.Unlock()
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

func (qs *QueueServer) Run(cancel <-chan interface{}) error {
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		fmt.Printf("Error Creating listener %v\n", err)
		return err
	}
	fmt.Println("Created Listener")
	go func() {
		if err := qs.gpServer.Serve(listener); err != nil {
			fmt.Printf("Error Starting grpc serve %v", err)
		}
		log.Printf("GRPC server listening on %s", qs.port)

	}()
	<-cancel
	_ = listener.Close()
	qs.gpServer.Stop()
	return nil
}
