package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAppendToTheSegment(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_10", os.TempDir()))

	index := NewIndex()

	segment, err := NewSegment(10, config, index)
	assert.NoError(t, err)
	defer segment.Close()

	id, err := segment.Append([]byte("Hello World"))
	assert.NoError(t, err)
	assert.Equal(t, id, 0)
	_, ok := index.entries[0]
	assert.True(t, ok)
}

func TestReadFromTheSegment(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_10", os.TempDir()))

	index := NewIndex()

	segment, err := NewSegment(10, config, index)
	assert.NoError(t, err)
	defer segment.Close()

	id1, err := segment.Append([]byte("Hello World"))
	assert.NoError(t, err)

	id2, err := segment.Append([]byte("Bye World"))
	assert.NoError(t, err)

	data2, err := segment.Read(id2)
	assert.NoError(t, err)
	assert.Equal(t, []byte("Bye World"), data2)

	data1, err := segment.Read(id1)
	assert.NoError(t, err)
	assert.Equal(t, []byte("Hello World"), data1)
}

func TestReadNonExistingMessage(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_12", os.TempDir()))

	index := NewIndex()

	segment, err := NewSegment(12, config, index)
	assert.NoError(t, err)
	defer segment.Close()

	_, err = segment.Read(13)
	assert.Error(t, err)
}

func TestReadFromReloadedSegment(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_13", os.TempDir()))

	index := NewIndex()

	segment, err := NewSegment(12, config, index)
	assert.NoError(t, err)

	segment.Append([]byte("Hello world"))
	segment.Append([]byte("Hello earth"))
	segment.Append([]byte("Hello India"))
	segment.Close()

	restoredSegment, err := NewSegment(12, config, index)
	assert.NoError(t, err)

	data1, _ := restoredSegment.Read(0)
	data2, _ := restoredSegment.Read(1)
	data3, _ := restoredSegment.Read(2)

	assert.Equal(t, []byte("Hello world"), data1)
	assert.Equal(t, []byte("Hello earth"), data2)
	assert.Equal(t, []byte("Hello India"), data3)
}
