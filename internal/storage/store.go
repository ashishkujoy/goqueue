package storage

import (
	"encoding/binary"
	"os"
)

type Store struct {
	reader *os.File
	writer *os.File
	offset int
}

func (s *Store) CloseWriter() error {
	return s.writer.Close()
}

func NewStore(filePath string) (*Store, error) {
	writer, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := reader.Stat()
	if err != nil {
		return nil, err
	}

	return &Store{
		reader: reader,
		writer: writer,
		offset: int(stat.Size()),
	}, nil
}

// RestoreStore restores a store from a file at the given filePath.
// It opens the file for reading only
// TODO: Add error handling of writing to a restored store.(closed segment)
func RestoreStore(filePath string) (*Store, error) {
	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		reader: reader,
	}, nil
}

func (s *Store) Append(data []byte) (int, error) {
	currentOffset := s.offset
	sizeBuff := make([]byte, 4)

	binary.BigEndian.PutUint32(sizeBuff, uint32(len(data)))
	n, err := s.writer.Write(sizeBuff)
	if err != nil {
		return 0, err
	}
	s.offset += n

	n, err = s.writer.Write(data)
	if err != nil {
		return 0, err
	}
	s.offset += n

	return currentOffset, nil
}

func (s *Store) Read(offset int) ([]byte, error) {
	sizeBuff := make([]byte, 4)
	n, err := s.reader.ReadAt(sizeBuff, int64(offset))
	if err != nil {
		return nil, err
	}
	offset += n

	dataLen := binary.BigEndian.Uint32(sizeBuff)
	data := make([]byte, dataLen)

	_, err = s.reader.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}

	return data, err
}

func (s *Store) Flush() error {
	return s.writer.Sync()
}

func (s *Store) Close() error {
	if err := s.Flush(); err != nil {
		return err
	}
	if err := s.reader.Close(); err != nil {
		return err
	}

	return s.writer.Close()
}

func (s *Store) Size() int {
	return s.offset
}
