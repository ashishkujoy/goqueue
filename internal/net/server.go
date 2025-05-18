package netinternal

import (
	"ashishkujoy/queue/internal/config"
	queueinternal "ashishkujoy/queue/internal/queue"
	netinternal "ashishkujoy/queue/proto"
	context "context"
	"fmt"
	"log"
	"net"

	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

type QueueServer struct {
	netinternal.UnimplementedQueueServiceServer
	queueService *queueinternal.QueueService
	port         string
	gpServer     *grpc.Server
}

func NewQueueServer(config *config.Config, port string) (*QueueServer, error) {
	service, err := queueinternal.NewQueueService(config)
	if err != nil {
		return nil, err
	}

	gpServer := grpc.NewServer()
	server := &QueueServer{
		port:         port,
		queueService: service,
		gpServer:     gpServer,
	}
	netinternal.RegisterQueueServiceServer(gpServer, server)
	return server, nil
}

func (qs *QueueServer) Enqueue(ctx context.Context, req *netinternal.EnqueueRequest) (*netinternal.EnqueueRequestResponse, error) {
	if err := qs.queueService.Enqueue(req.Message); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to enqueue")
	}
	qs.broadcastMessage()
	return &netinternal.EnqueueRequestResponse{Success: true}, nil
}

func (qs *QueueServer) broadcastMessage() {
	// panic("unimplemented")
}

func (qs *QueueServer) ObserveQueue(req *netinternal.ObserveQueueRequest, stream grpc.ServerStreamingServer[netinternal.QueueMessage]) error {
	return qs.serveMessages(req, stream)
}

func (qs *QueueServer) serveMessages(req *netinternal.ObserveQueueRequest, stream grpc.ServerStreamingServer[netinternal.QueueMessage]) error {
	for {
		msg, err := qs.queueService.Dequeue(int(req.ConsumerId))
		if err != nil {
			break
		}
		fmt.Printf("Message dequeued: %v\n", msg)
		err = stream.Send(&netinternal.QueueMessage{Message: msg})
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
	listener.Close()
	qs.gpServer.Stop()
	return nil
}
