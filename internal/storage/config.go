package storage

type Config struct {
	segmentsRoot          string
	MetadataPath          string
	maxSegmentSizeInBytes int
}

func NewConfig(segmentsRoot string, maxSegmentSizeInBytes int) *Config {
	return &Config{
		segmentsRoot:          segmentsRoot,
		maxSegmentSizeInBytes: maxSegmentSizeInBytes,
	}
}

func NewConfigWithMetadataPath(segmentsRoot, metadataPath string, maxSegmentSizeInBytes int) *Config {
	return &Config{
		segmentsRoot:          segmentsRoot,
		MetadataPath:          metadataPath,
		maxSegmentSizeInBytes: maxSegmentSizeInBytes,
	}
}
