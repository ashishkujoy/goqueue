package storage

import (
	"ashishkujoy/queue/internal/config"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createTempDir(suffix string) string {
	dir := os.TempDir() + "/" + suffix
	os.MkdirAll(dir, 0755)
	return dir
}

func removeTempDir(suffix string) {
	dir := os.TempDir() + "/" + suffix
	os.RemoveAll(dir)
}

func TestAppend(t *testing.T) {
	cfg := config.NewConfig(
		createTempDir("SegmentTestAppend"),
		createTempDir("metadata"),
		1000,
		time.Second,
	)
	defer removeTempDir("SegmentTestAppend")
	defer removeTempDir("metadata")
	index, err := NewIndex(cfg)
	assert.NoError(t, err)
	segments, err := NewSegments(cfg, index)

	assert.NoError(t, err)

	messageId, err := segments.Append([]byte("Hello Segments"))
	assert.NoError(t, err)

	data, err := segments.Read(messageId)
	assert.NoError(t, err)
	assert.Equal(t, []byte("Hello Segments"), data)
}

func TestAppendMultipleEntry(t *testing.T) {
	cfg := config.NewConfig(
		createTempDir("SegmentTestAppendMultipleEntry"),
		createTempDir("metadata"),
		1000,
		time.Second,
	)
	defer removeTempDir("SegmentTestAppendMultipleEntry")
	defer removeTempDir("metadata")
	index, _ := NewIndex(cfg)
	segments, err := NewSegments(cfg, index)
	assert.NoError(t, err)

	messageId1, err := segments.Append([]byte("Hello Segments"))
	assert.NoError(t, err)

	messageId2, err := segments.Append([]byte("Another Hello Segments"))
	assert.NoError(t, err)

	data2, err := segments.Read(messageId2)
	assert.NoError(t, err)
	assert.Equal(t, "Another Hello Segments", string(data2))

	data1, err := segments.Read(messageId1)
	assert.NoError(t, err)
	assert.Equal(t, []byte("Hello Segments"), data1)
}

func TestSegmentRollOver(t *testing.T) {
	cfg := config.NewConfig(
		createTempDir("TestSegmentRollOver1"),
		createTempDir("metadata"),
		20,
		time.Second,
	)
	defer removeTempDir("TestSegmentRollOver1")
	index, _ := NewIndex(cfg)
	segments, err := NewSegments(cfg, index)
	assert.NoError(t, err)

	segments.Append([]byte("Hello Segments"))
	assert.Equal(t, 0, len(segments.closedSegments))

	segments.Append([]byte("Another Hello Segments"))
	assert.NoError(t, err)
	assert.Equal(t, 1, len(segments.closedSegments))
}

func TestReadFromARolledOverSegment(t *testing.T) {
	cfg := config.NewConfig(
		createTempDir("TestReadFromARolledOverSegment"),
		createTempDir("metadata"),
		10,
		time.Second,
	)
	defer removeTempDir("TestReadFromARolledOverSegment")
	index, _ := NewIndex(cfg)
	segments, err := NewSegments(cfg, index)
	assert.NoError(t, err)

	messageId1, _ := segments.Append([]byte("Hello Segments"))
	messageId2, _ := segments.Append([]byte("Another Hello Segments"))

	assert.Equal(t, 1, len(segments.closedSegments))

	data2, _ := segments.Read(messageId2)
	data1, _ := segments.Read(messageId1)
	assert.Equal(t, []byte("Another Hello Segments"), data2)
	assert.Equal(t, []byte("Hello Segments"), data1)
}

func TestRestoreASegment(t *testing.T) {
	cfg := config.NewConfig(
		createTempDir("TestRestoreASegment"),
		createTempDir("metadata"),
		10,
		time.Second,
	)
	defer removeTempDir("TestRestoreASegment")
	index, _ := NewIndex(cfg)
	segments, err := NewSegments(cfg, index)
	assert.NoError(t, err)

	messageId1, _ := segments.Append([]byte("Hello Segments"))
	messageId2, _ := segments.Append([]byte("Another Hello Segments"))
	messageId3, _ := segments.Append([]byte("Yet Another Hello Segments"))
	_ = segments.Close()
	restoreSegments, err := RestoreSegments(cfg, index)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(restoreSegments.closedSegments))

	data2, _ := restoreSegments.Read(messageId2)
	data1, _ := restoreSegments.Read(messageId1)
	data3, _ := restoreSegments.Read(messageId3)
	assert.Equal(t, []byte("Another Hello Segments"), data2)
	assert.Equal(t, []byte("Hello Segments"), data1)
	assert.Equal(t, []byte("Yet Another Hello Segments"), data3)
}
