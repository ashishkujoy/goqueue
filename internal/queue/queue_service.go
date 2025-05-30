package queueinternal

import (
	"ashishkujoy/queue/internal/config"
	"ashishkujoy/queue/internal/consumer"
)

type QueueService struct {
	queue         *Queue
	consumerIndex *consumer.ConsumerIndex
}

func NewQueueService(config *config.Config) (*QueueService, error) {
	queue, err := RestoreQueue(config)
	if err != nil {
		return nil, err
	}
	consumerIndex, err := consumer.RestoreConsumerIndex(config)
	if err != nil {
		return nil, err
	}

	return &QueueService{
		queue:         queue,
		consumerIndex: consumerIndex,
	}, nil
}

func (qs *QueueService) Enqueue(data []byte) error {
	_, err := qs.queue.Enqueue(data)
	return err
}

func (qs *QueueService) Dequeue(consumerId int) ([]byte, error) {
	index := qs.consumerIndex.ReadIndex(consumerId)
	data, err := qs.queue.Dequeue(index + 1)
	if err != nil {
		return nil, err
	}
	qs.consumerIndex.WriteIndex(consumerId, index+1)
	return data, nil
}

func (qs *QueueService) RevertDequeue(consumerId int) {
	index := qs.consumerIndex.ReadIndex(consumerId)
	qs.consumerIndex.WriteIndex(consumerId, index-1)
}

func (qs *QueueService) Close() error {
	err := qs.queue.Close()
	if err != nil {
		return err
	}
	return qs.consumerIndex.Close()
}
