package config

type Config struct {
	segmentsRoot          string
	MetadataPath          string
	maxSegmentSizeInBytes int
}

func (c *Config) MaxSegmentSizeInBytes() int {
	return c.maxSegmentSizeInBytes
}

func (c *Config) SegmentsRoot() string {
	return c.segmentsRoot
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
