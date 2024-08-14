package raftpebbledb

type PebbleDBConfig struct {
	KVLRUCacheSize                   int64
	KVWriteBufferSize                uint64
	KVMaxWriteBufferNumber           int
	KVLevel0FileNumCompactionTrigger int
	KVLevel0StopWritesTrigger        int
	KVMaxBytesForLevelBase           int64
	KVTargetFileSizeBase             int64
	KVTargetFileSizeMultiplier       int64
	KVNumOfLevels                    int
	KVMaxOpenFiles                   int
	KVMaxConcurrentCompactions       int
	KVBlockSize                      int
	KVMaxManifestFileSize            int64
	KVBytesPerSync                   int
	KVWALBytesPerSync                int
}

func DefaultPebbleDBConfig() *PebbleDBConfig {
	return &PebbleDBConfig{
		KVLRUCacheSize:                   128 * 1024 * 1024, // 128MB
		KVWriteBufferSize:                32 * 1024 * 1024,  // 32MB
		KVMaxWriteBufferNumber:           4,
		KVLevel0FileNumCompactionTrigger: 1,
		KVLevel0StopWritesTrigger:        24,
		KVMaxBytesForLevelBase:           512 * 1024 * 1024, // 512MB
		KVTargetFileSizeBase:             128 * 1024 * 1024, // 128MB
		KVTargetFileSizeMultiplier:       1,
		KVNumOfLevels:                    7,
		KVMaxOpenFiles:                   102400,
		KVMaxConcurrentCompactions:       8,
		KVBlockSize:                      64 * 1024,         // 64KB
		KVMaxManifestFileSize:            128 * 1024 * 1024, // 128MB
		KVBytesPerSync:                   2 * 1024 * 1024,   // 2MB
		KVWALBytesPerSync:                2 * 1024 * 1024,   // 2MB
	}
}
