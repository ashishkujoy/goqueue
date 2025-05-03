package storage

type MessageEntry struct {
	segmentId int
	offset    int
}

func NewMessageEntry(segmentId int, offset int) MessageEntry {
	return MessageEntry{segmentId: segmentId, offset: offset}
}

type Index struct {
	entries   map[int]MessageEntry
	elementId int
}

func NewIndex() *Index {
	return &Index{
		entries:   make(map[int]MessageEntry),
		elementId: 0,
	}
}

func (i *Index) Append(messageEntry MessageEntry) int {
	currentElementId := i.elementId
	i.entries[currentElementId] = messageEntry
	i.elementId++
	return currentElementId
}

func (i *Index) GetOffset(elementId int) (MessageEntry, bool) {
	v, ok := i.entries[elementId]
	return v, ok
}
