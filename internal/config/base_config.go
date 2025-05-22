package config

import "time"

type Config struct {
	segmentsRoot              string
	MetadataPath              string
	consumerIndexSyncInterval time.Duration
	maxSegmentSizeInBytes     int
}

func (c *Config) MaxSegmentSizeInBytes() int {
	return c.maxSegmentSizeInBytes
}

func (c *Config) SegmentsRoot() string {
	return c.segmentsRoot
}

func (c *Config) ConsumerIndexSyncInterval() time.Duration {
	return c.consumerIndexSyncInterval
}

func NewConfig(
	segmentsRoot string,
	metadataPath string,
	maxSegmentSizeInBytes int,
	consumerIndexSyncInterval time.Duration,
) *Config {
	return &Config{
		segmentsRoot:              segmentsRoot,
		maxSegmentSizeInBytes:     maxSegmentSizeInBytes,
		MetadataPath:              metadataPath,
		consumerIndexSyncInterval: consumerIndexSyncInterval,
	}
}
