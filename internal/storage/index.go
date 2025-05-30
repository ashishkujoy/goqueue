package storage

import (
	"ashishkujoy/queue/internal/config"
	"encoding/binary"
	"sync"
)

type MessageEntry struct {
	segmentId int
	offset    int
	elementId int
}

func (m *MessageEntry) Encode() []byte {
	data := make([]byte, 24)
	offset := 0
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(m.segmentId))
	offset += 8
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(m.offset))
	offset += 8
	binary.BigEndian.PutUint64(data[offset:offset+8], uint64(m.offset))
	return data
}

func (m *MessageEntry) Decode(data []byte) {
	offset := 0
	m.segmentId = int(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	m.offset = int(binary.BigEndian.Uint64(data[offset : offset+8]))
	offset += 8
	m.elementId = int(binary.BigEndian.Uint64(data[offset : offset+8]))
}

func NewMessageEntry(segmentId int, offset int) MessageEntry {
	return MessageEntry{segmentId: segmentId, offset: offset}
}

type Index struct {
	entries   map[int]MessageEntry
	elementId int
	store     *Store
	mu        *sync.Mutex
}

func NewIndex(cfg *config.Config) (*Index, error) {
	store, err := NewStore(cfg.IndexFilePath())
	if err != nil {
		return nil, err
	}
	return &Index{
		entries:   make(map[int]MessageEntry),
		store:     store,
		elementId: 0,
		mu:        &sync.Mutex{},
	}, nil
}

func RestoreIndex(cfg *config.Config) (*Index, error) {
	store, err := NewStore(cfg.IndexFilePath())
	if err != nil {
		return nil, err
	}
	entries, elementId, err := restoreEntries(store)
	if err != nil {
		return nil, err
	}
	return &Index{
		entries:   entries,
		store:     store,
		elementId: elementId,
		mu:        &sync.Mutex{},
	}, nil
}

func restoreEntries(store *Store) (map[int]MessageEntry, int, error) {
	entriesBytes, err := store.readAllEntries()
	entries := make(map[int]MessageEntry)
	if err != nil {
		return nil, 0, err
	}
	var elementId = 0
	for _, entry := range entriesBytes {
		messageEntry := MessageEntry{}
		messageEntry.Decode(entry)
		entries[messageEntry.elementId] = messageEntry
		elementId = messageEntry.elementId
	}
	return entries, elementId, nil
}

func (i *Index) Append(messageEntry MessageEntry) (int, error) {
	currentElementId := i.elementId
	messageEntry.elementId = currentElementId
	i.entries[currentElementId] = messageEntry
	i.elementId++
	i.mu.Lock()
	defer i.mu.Unlock()
	_, err := i.store.Append(messageEntry.Encode())
	if err != nil {
		return 0, err
	}
	return currentElementId, nil
}

func (i *Index) GetOffset(elementId int) (MessageEntry, bool) {
	v, ok := i.entries[elementId]
	return v, ok
}
