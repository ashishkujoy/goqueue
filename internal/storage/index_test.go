package storage

import (
	"ashishkujoy/queue/internal/config"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCreateNewIndex(t *testing.T) {
	cfg := config.NewConfig(
		"",
		createTempDir("TestCreateNewIndex"),
		1000,
		0,
	)
	defer removeTempDir("TestCreateNewIndex")
	index, err := NewIndex(cfg)
	assert.NoError(t, err)

	assert.Equal(t, 0, len(index.entries))
}

func TestAppendToAIndex(t *testing.T) {
	cfg := config.NewConfig(
		"",
		createTempDir("TestAppendToAIndex"),
		1000,
		0,
	)
	defer removeTempDir("TestAppendToAIndex")
	index, _ := NewIndex(cfg)

	_, _ = index.Append(MessageEntry{segmentId: 0, offset: 0})
	_, _ = index.Append(MessageEntry{segmentId: 1, offset: 10})

	assert.Equal(t, 2, len(index.entries))
}

func TestReadFromAIndex(t *testing.T) {
	cfg := config.NewConfig(
		"",
		createTempDir("TestReadFromAIndex"),
		1000,
		0,
	)
	defer removeTempDir("TestReadFromAIndex")
	index, _ := NewIndex(cfg)

	i1, _ := index.Append(MessageEntry{segmentId: 0, offset: 0})
	i2, _ := index.Append(MessageEntry{segmentId: 1, offset: 10})

	offset1, _ := index.GetOffset(i1)
	offset2, _ := index.GetOffset(i2)

	assert.Equal(t, MessageEntry{segmentId: 0, offset: 0, elementId: i1}, offset1)
	assert.Equal(t, MessageEntry{segmentId: 1, offset: 10, elementId: i2}, offset2)
}

func TestReadFromARestoredIndex(t *testing.T) {
	cfg := config.NewConfig(
		"",
		createTempDir("TestReadFromARestoredIndex"),
		1000,
		0,
	)
	defer removeTempDir("TestReadFromARestoredIndex")
	index, _ := NewIndex(cfg)

	i1, _ := index.Append(MessageEntry{segmentId: 0, offset: 0})
	i2, _ := index.Append(MessageEntry{segmentId: 1, offset: 10})

	assert.NoError(t, index.Close())
	index, _ = RestoreIndex(cfg)

	offset1, _ := index.GetOffset(i1)
	offset2, _ := index.GetOffset(i2)

	assert.Equal(t, MessageEntry{segmentId: 0, offset: 0, elementId: i1}, offset1)
	assert.Equal(t, MessageEntry{segmentId: 1, offset: 10, elementId: i2}, offset2)
}
