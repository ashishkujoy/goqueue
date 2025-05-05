package queueinternal

import (
	"ashishkujoy/queue/internal/config"
	"os"
	"testing"

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

func TestEnqueueSingleElement(t *testing.T) {
	config := config.NewConfig(createTempDir("TestEnqueueSingleElement"), 1000)
	defer removeTempDir("TestEnqueueSingleElement")
	queue, err := NewQueue(config)
	assert.NoError(t, err)
	defer queue.Close()

	id, err := queue.Enqueue([]byte("First Message"))
	assert.NoError(t, err)

	data, err := queue.Dequeue(id)
	assert.NoError(t, err)
	assert.Equal(t, data, []byte("First Message"))
}
