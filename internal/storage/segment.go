package storage

import (
	"fmt"
)

type Segment struct {
	store *Store
	index *Index
}

func NewSegment(startingIndex int, config *Config, index *Index) (*Segment, error) {
	filePath := fmt.Sprintf("%s/segment-%d", config.segmentsRoot, startingIndex)
	store, err := NewStore(filePath)
	if err != nil {
		return nil, err
	}

	return &Segment{store: store, index: index}, nil
}

func (s *Segment) Append(data []byte) (int, error) {
	offset, err := s.store.Append(data)
	if err != nil {
		return 0, err
	}
	elementIndex := s.index.Append(offset)
	return elementIndex, nil
}

func (s *Segment) Read(id int) ([]byte, error) {
	offset, ok := s.index.GetOffset(id)
	if !ok {
		return nil, fmt.Errorf("unknown message id: %d", id)
	}
	return s.store.Read(offset)
}

func (s *Segment) Close() error {
	return s.store.Close()
}
