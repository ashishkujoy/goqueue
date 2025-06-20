package consumer

import (
	"ashishkujoy/queue/internal"
	"ashishkujoy/queue/internal/config"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ConsumerIndex manages the index for consumers.
// It provides methods to read and write the index for each consumer.
// It uses a mutex to ensure thread safety while accessing the index.
// The index is stored in a file, and the file is created when the consumer is initialized.
type ConsumerIndex struct {
	writer  *os.File
	mu      *sync.RWMutex
	config  *config.Config
	indexes map[int]int
}

// NewConsumerIndex initializes a new ConsumerIndex instance.
func NewConsumerIndex(config *config.Config) (*ConsumerIndex, error) {
	writer, err := createIndexFile(config)
	if err != nil {
		return nil, err
	}

	consumerIndex := &ConsumerIndex{
		writer:  writer,
		mu:      &sync.RWMutex{},
		indexes: make(map[int]int),
		config:  config,
	}
	return consumerIndex, nil
}

func (ci *ConsumerIndex) schedulePersist() {
	fmt.Printf("Starting consumer index persistence with interval: %s\n", ci.config.ConsumerIndexSyncInterval())
	ticker := time.NewTicker(ci.config.ConsumerIndexSyncInterval())
	go func() {
		for {
			select {
			case <-ticker.C:
				fmt.Printf("Starting consumer index persistence\n")
				err := ci.Persist()
				if err != nil {
					fmt.Printf("Error persisting consumer index: %v\n", err)
				}
				fmt.Printf("Done with consumer index persistence\n")
			}
		}
	}()
}

func extractTimestamp(filename string) int64 {
	prefix := "consumer_index_"
	index := strings.LastIndex(filename, prefix)
	num, _ := strconv.ParseInt(filename[index+len(prefix):], 10, 64)
	return num
}

func getLastIndexFile(config *config.Config) (*os.File, error) {
	files, err := getSortedIndexFiles(config)
	if err != nil {
		return nil, err
	}
	return files[0], nil
}

func getSortedIndexFiles(config *config.Config) ([]*os.File, error) {
	entries, err := os.ReadDir(config.MetadataPath)
	if err != nil {
		return nil, err
	}
	var metadataTimestamp []int64
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "consumer_index_") {
			metadataTimestamp = append(metadataTimestamp, extractTimestamp(entry.Name()))
		}
	}
	if len(metadataTimestamp) == 0 {
		return nil, fmt.Errorf("no index file found")
	}
	sort.Slice(metadataTimestamp, func(i, j int) bool {
		return metadataTimestamp[i] > metadataTimestamp[j]
	})
	files := internal.Map(metadataTimestamp, func(i int64) *os.File {
		file, err := os.OpenFile(fmt.Sprintf("%s/consumer_index_%d", config.MetadataPath, i), os.O_RDWR, 0666)
		if err != nil {
			return nil
		}
		return file
	})
	return files, nil
}

func restoreIndexesFromFile(file *os.File) (map[int]int, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	indexSize := stat.Size() / 8
	offset := 0
	indexes := make(map[int]int, stat.Size()/8)
	for i := int64(0); i < indexSize; i++ {
		buf := make([]byte, 4)
		_, err := file.ReadAt(buf, int64(offset))
		if err != nil {
			return nil, err
		}
		offset += 4
		consumerId := binary.BigEndian.Uint32(buf)
		buf = make([]byte, 4)
		_, err = file.ReadAt(buf, int64(offset))
		if err != nil {
			return nil, err
		}
		offset += 4
		consumerIndex := binary.BigEndian.Uint32(buf)
		indexes[int(consumerId)] = int(consumerIndex)
	}
	return indexes, nil
}

func RestoreConsumerIndex(config *config.Config) (*ConsumerIndex, error) {
	lastIndexFile, err := getLastIndexFile(config)
	if err != nil {
		lastIndexFilePath := fmt.Sprintf("%s/consumer_index_%d", config.MetadataPath, time.Now().Unix())
		lastIndexFile, err = os.OpenFile(lastIndexFilePath, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			return nil, err
		}
	}
	indexes, err := restoreIndexesFromFile(lastIndexFile)
	if err != nil {
		return nil, err
	}

	c := &ConsumerIndex{
		writer:  lastIndexFile,
		indexes: indexes,
		mu:      &sync.RWMutex{},
		config:  config,
	}
	go c.schedulePersist()
	return c, nil
}

// createIndexFile creates a new index file for the consumer.
func createIndexFile(config *config.Config) (*os.File, error) {
	filepath := fmt.Sprintf("%s/consumer_index_%d", config.MetadataPath, time.Now().Unix())
	writer, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	return writer, nil
}

// WriteIndex updates the index for a given consumer ID.
// It uses a write lock to ensure thread safety while accessing the index.
func (ci *ConsumerIndex) WriteIndex(consumerId, index int) {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	ci.indexes[consumerId] = index
}

// ReadIndex retrieves the index for a given consumer ID.
// It uses a read lock to ensure thread safety while accessing the index.
// If the consumer ID does not exist in the index, it initializes it to -1 and returns -1.
// The consumer of this function should increment the index and read record at that index.
func (ci *ConsumerIndex) ReadIndex(consumerId int) int {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	index, exists := ci.indexes[consumerId]
	if !exists {
		ci.indexes[consumerId] = -1
		return -1
	}
	return index
}

// Sync synchronizes the consumer index with the underlying storage.
// It creates a snapshot of the current consumer index and writes it to the index file.
// It uses a write lock to ensure thread safety while accessing the index.
// If an error occurs during the write operation, it returns the error.
// Otherwise, it updates the writer to point to the new index file.
// The old index file is not deleted, but it can be managed separately if needed.
// This function is typically called periodically to ensure that the consumer index is up to date.
func (ci *ConsumerIndex) Sync() error {
	snapshot := ci.CreateSnapshot()
	newWriter, err := createIndexFile(ci.config)
	if err != nil {
		return err
	}
	_, err = newWriter.Write(snapshot)
	if err != nil {
		return err
	}
	ci.mu.Lock()
	defer ci.mu.Unlock()
	ci.writer = newWriter

	return nil
}

// CreateSnapshot creates a snapshot of the current consumer index.
// It returns a byte slice containing the serialized consumer index data.
// It uses a read lock to ensure thread safety while accessing the index.
func (ci *ConsumerIndex) CreateSnapshot() []byte {
	ci.mu.RLock()
	defer ci.mu.RUnlock()

	size := len(ci.indexes)
	buf := make([]byte, size*8)
	offset := 0
	for consumerId, consumerIndex := range ci.indexes {
		binary.BigEndian.PutUint32(buf[offset:], uint32(consumerId))
		offset += 4
		binary.BigEndian.PutUint32(buf[offset:], uint32(consumerIndex))
		offset += 4
	}

	return buf
}

func (ci *ConsumerIndex) Close() error {
	return ci.Persist()
}

func removeOldIndexFiles(config *config.Config, currentIndexFile string) error {
	entries, err := os.ReadDir(config.MetadataPath)
	if err != nil {
		return err
	}

	consumerIndexes := internal.Filter(entries, func(entry fs.DirEntry) bool {
		return strings.Contains(entry.Name(), "consumer_index_") && !strings.Contains(currentIndexFile, entry.Name())
	})

	for _, entry := range consumerIndexes {
		dir, _ := os.Getwd()

		fmt.Printf("Current working dir: %s\n", dir)
		err := os.Remove(fmt.Sprintf("%s/%s", config.MetadataPath, entry.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (ci *ConsumerIndex) Persist() error {
	snapshot := ci.CreateSnapshot()
	indexFile, err := createIndexFile(ci.config)
	if err != nil {
		return err
	}
	_, err = indexFile.Write(snapshot)
	err = indexFile.Sync()
	if err != nil {
		return err
	}
	ci.writer = indexFile
	err = removeOldIndexFiles(ci.config, indexFile.Name())
	if err != nil {
		return err
	}
	return err
}
