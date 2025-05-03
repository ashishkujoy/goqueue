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

	segment, err := NewSegment(10, config)
	assert.NoError(t, err)
	defer segment.Close()

	_, err = segment.Append([]byte("Hello World"))
	assert.NoError(t, err)
}

func TestReadFromTheSegment(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_10", os.TempDir()))

	segment, err := NewSegment(10, config)
	assert.NoError(t, err)
	defer segment.Close()

	offset1, err := segment.Append([]byte("Hello World"))
	assert.NoError(t, err)

	offset2, err := segment.Append([]byte("Bye World"))
	assert.NoError(t, err)

	data2, err := segment.Read(offset2)
	assert.NoError(t, err)
	assert.Equal(t, []byte("Bye World"), data2)

	data1, err := segment.Read(offset1)
	assert.NoError(t, err)
	assert.Equal(t, []byte("Hello World"), data1)
}

func TestReadNonExistingMessage(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_12", os.TempDir()))

	segment, err := NewSegment(12, config)
	assert.NoError(t, err)
	defer segment.Close()

	_, err = segment.Read(13)
	assert.Error(t, err)
}

func TestReadFromReloadedSegment(t *testing.T) {
	config := &Config{segmentsRoot: os.TempDir()}
	defer os.Remove(fmt.Sprintf("%s/segment_13", os.TempDir()))

	segment, err := NewSegment(12, config)
	assert.NoError(t, err)

	offset1, _ := segment.Append([]byte("Hello world"))
	offset2, _ := segment.Append([]byte("Hello earth"))
	offset3, _ := segment.Append([]byte("Hello India"))
	segment.Close()

	restoredSegment, err := NewSegment(12, config)
	assert.NoError(t, err)

	data1, _ := restoredSegment.Read(offset1)
	data2, _ := restoredSegment.Read(offset2)
	data3, _ := restoredSegment.Read(offset3)

	assert.Equal(t, []byte("Hello world"), data1)
	assert.Equal(t, []byte("Hello earth"), data2)
	assert.Equal(t, []byte("Hello India"), data3)
}
