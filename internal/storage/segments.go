package storage

import (
	"fmt"
	"sync"
)

// Segments manages multiple segments.
// It is responsible for appending data to the active segment,
// reading data from segments, and rolling over to a new segment
// when the current segment is full.
type Segments struct {
	config         *Config
	active         *Segment
	id             int
	index          *Index
	closedSegments []*Segment
	mu             *sync.Mutex
}

// NewSegments creates a new Segments instance with the given configuration and index.
func NewSegments(config *Config, index *Index) (*Segments, error) {
	segment, err := NewSegment(0, config)
	if err != nil {
		return nil, err
	}

	return &Segments{
		config:         config,
		active:         segment,
		index:          index,
		closedSegments: make([]*Segment, 0),
		mu:             &sync.Mutex{},
	}, nil
}

// Append appends data to the active segment.
// If the active segment is full, it rolls over to a new segment.
func (s *Segments) Append(data []byte) (int, error) {
	if s.active.isFull(s.config.maxSegmentSizeInBytes) {
		if err := s.rollOverSegment(); err != nil {
			return 0, err
		}
	}
	offset, err := s.active.Append(data)
	if err != nil {
		return 0, err
	}
	messageId := s.index.Append(NewMessageEntry(s.active.id, offset))
	return messageId, nil
}

// Read reads data from the segment with the given message ID.
// It retrieves the offset from the index and reads the data from the corresponding segment.
// If the message ID is unknown, it returns an error.
// If the segment is not found, it returns an error.
// It returns the data read from the segment or an error if the operation fails.
// It locks the segment for reading to ensure thread safety.
func (s *Segments) Read(messageId int) ([]byte, error) {
	entry, ok := s.index.GetOffset(messageId)
	if !ok {
		return nil, fmt.Errorf("unknown message id: %d", messageId)
	}
	segment, err := s.findSegment(entry.segmentId)
	if err != nil {
		return nil, err
	}
	return segment.Read(entry.offset)
}

// Flush flushes any pending writes to the active segment.
func (s *Segments) Flush() {
	s.active.store.Flush()
}

// findSegment finds a segment by its ID.
func (s *Segments) findSegment(segmentId int) (*Segment, error) {
	if s.active.id == segmentId {
		return s.active, nil
	}
	for _, segment := range s.closedSegments {
		if segment.id == segmentId {
			return segment, nil
		}
	}
	return nil, fmt.Errorf("unknown segment %d", segmentId)
}

// rollOverSegment rolls over to a new segment.
// It closes the current active segment writer,
// appends it to the closed segments list, and creates a new active segment.
func (s *Segments) rollOverSegment() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active.CloseWriter()
	s.closedSegments = append(s.closedSegments, s.active)
	s.id++
	newActiveSegment, err := NewSegment(s.id, s.config)
	if err != nil {
		return err
	}
	s.active = newActiveSegment
	return nil
}

// Close closes the active segment and all closed segments.
// It flushes any pending writes to the store and releases resources.
func (s *Segments) Close() error {
	if err := s.active.Close(); err != nil {
		return err
	}
	for _, segment := range s.closedSegments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}
