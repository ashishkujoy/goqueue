package storage

type Index struct {
	entries   map[int]int
	elementId int
}

func NewIndex() *Index {
	return &Index{
		entries:   make(map[int]int),
		elementId: 0,
	}
}

func (i *Index) Append(offset int) int {
	currentElementId := i.elementId
	i.entries[currentElementId] = offset
	i.elementId++
	return currentElementId
}

func (i *Index) GetOffset(elementId int) (int, bool) {
	v, ok := i.entries[elementId]
	return v, ok
}
