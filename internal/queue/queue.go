package queueinternal

import (
	"ashishkujoy/queue/internal/config"
	"ashishkujoy/queue/internal/storage"
)

type Queue struct {
	segments *storage.Segments
}

func NewQueue(config *config.Config) (*Queue, error) {
	segments, err := storage.NewSegments(config, storage.NewIndex())
	if err != nil {
		return nil, err
	}
	return &Queue{segments: segments}, nil
}

func RestoreQueue(config *config.Config) (*Queue, error) {
	segments, err := storage.RestoreSegments(config, storage.NewIndex())
	if err != nil {
		return nil, err
	}
	return &Queue{segments: segments}, nil
}

func (q *Queue) Enqueue(data []byte) (int, error) {
	return q.segments.Append(data)
}

func (q *Queue) Dequeue(id int) ([]byte, error) {
	return q.segments.Read(id)
}

func (q *Queue) Close() error {
	return q.segments.Close()
}
