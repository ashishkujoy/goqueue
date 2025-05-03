package storage

type Config struct {
	segmentsRoot          string
	maxSegmentSizeInBytes int
}

func NewConfig(segmentsRoot string, maxSegmentSizeInBytes int) *Config {
	return &Config{
		segmentsRoot:          segmentsRoot,
		maxSegmentSizeInBytes: maxSegmentSizeInBytes,
	}
}
