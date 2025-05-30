package queueinternal

import (
	"ashishkujoy/queue/internal/config"
	"ashishkujoy/queue/internal/storage"
)

type Queue struct {
	segments *storage.Segments
}

func NewQueue(cfg *config.Config) (*Queue, error) {
	index, err := storage.NewIndex(cfg)
	if err != nil {
		return nil, err
	}
	segments, err := storage.NewSegments(cfg, index)
	if err != nil {
		return nil, err
	}
	return &Queue{segments: segments}, nil
}

func RestoreQueue(cfg *config.Config) (*Queue, error) {
	index, err := storage.RestoreIndex(cfg)
	if err != nil {
		return nil, err
	}
	segments, err := storage.RestoreSegments(cfg, index)
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
