package storage

import (
	"fmt"
)

type Segment struct {
	id    int
	store *Store
}

func (s *Segment) CloseWriter() error {
	return s.store.CloseWriter()
}

func NewSegment(id int, config *Config) (*Segment, error) {
	filePath := fmt.Sprintf("%s/segment-%d", config.segmentsRoot, id)
	store, err := NewStore(filePath)
	if err != nil {
		return nil, err
	}

	return &Segment{store: store, id: id}, nil
}

func (s *Segment) Append(data []byte) (int, error) {
	return s.store.Append(data)
}

func (s *Segment) Read(offset int) ([]byte, error) {
	return s.store.Read(offset)
}

func (s *Segment) isFull(maxSizeInBytes int) bool {
	return float64(s.store.Size()) >= float64(maxSizeInBytes)*0.9
}

func (s *Segment) Close() error {
	return s.store.Close()
}
