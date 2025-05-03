package storage

import "fmt"

type Segments struct {
	config         *Config
	active         *Segment
	id             int
	index          *Index
	closedSegments []*Segment
}

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
	}, nil
}

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

func (s *Segments) Flush() {
	s.active.store.Flush()
}

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

func (s *Segments) rollOverSegment() error {
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
