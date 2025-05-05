package storage

import (
	"ashishkujoy/queue/internal/config"
	"fmt"
	"sync"
)

// Segment represents a single segment in the queue.
// It is responsible for storing messages and managing the
// underlying storage.
// Each segment has a unique ID and is associated with a store.
// The segment is thread-safe and uses a mutex to synchronize access.
// The segment can be closed, and it will flush any pending writes to the store.
type Segment struct {
	id    int
	store *Store
	mu    *sync.RWMutex
}

// NewSegment creates a new segment with the given ID and configuration.
func NewSegment(id int, config *config.Config) (*Segment, error) {
	filePath := fmt.Sprintf("%s/segment-%d", config.SegmentsRoot(), id)
	store, err := NewStore(filePath)
	if err != nil {
		return nil, err
	}

	return &Segment{store: store, id: id, mu: &sync.RWMutex{}}, nil
}

// Append appends data to the segment.
// It locks the segment for writing to ensure thread safety.
// It returns the offset of the appended data or an error if the operation fails.
func (s *Segment) Append(data []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Append(data)
}

// Read reads data from the segment at the given offset.
// It locks the segment for reading to ensure thread safety.
// It returns the data read from the segment or an error if the operation fails.
func (s *Segment) Read(offset int) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.store.Read(offset)
}

// isFull checks if the segment is full based on the maximum size in bytes.
// It returns true if the segment is full, false otherwise.
// It uses a threshold of 90% of the maximum size to determine if the segment is full.
func (s *Segment) isFull(maxSizeInBytes int) bool {
	return float64(s.store.Size()) >= float64(maxSizeInBytes)*0.9
}

// Close closes the segment and flushes any pending writes to the store.
// It locks the segment for writing to ensure thread safety.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Close()
}

// Flush flushes any pending writes to the store.
func (s *Segment) CloseWriter() error {
	return s.store.CloseWriter()
}
