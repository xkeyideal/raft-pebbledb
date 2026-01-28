package raftpebbledb

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// CompressionType defines the compression algorithm used for SSTable blocks.
type CompressionType int

const (
	// CompressionNone disables compression.
	CompressionNone CompressionType = iota
	// CompressionSnappy uses Snappy compression (fast, moderate compression ratio).
	CompressionSnappy
	// CompressionZstd uses Zstandard compression (slower, better compression ratio).
	CompressionZstd
)

// PebbleDBConfig contains configuration options for the Pebble database.
// The defaults are tuned for Raft log storage workloads based on CockroachDB's
// production settings.
type PebbleDBConfig struct {
	// ---- Cache Settings ----

	// KVLRUCacheSize is the size of the shared block cache for uncompressed blocks.
	// Larger values improve read performance but consume more memory.
	// Default: 128MB. CockroachDB typically uses 512MB-1GB for production.
	KVLRUCacheSize int64

	// ---- MemTable Settings ----

	// KVWriteBufferSize (MemTableSize) is the size of a single memtable.
	// Larger values improve write throughput but increase memory usage and
	// recovery time after a crash.
	// Default: 64MB (CockroachDB recommendation).
	KVWriteBufferSize uint64

	// KVMaxWriteBufferNumber (MemTableStopWritesThreshold) is the maximum number
	// of memtables that can exist before writes are stopped.
	// Default: 4 (matches CockroachDB).
	KVMaxWriteBufferNumber int

	// ---- L0 Compaction Settings ----

	// KVLevel0FileNumCompactionTrigger (L0CompactionThreshold) is the number of
	// L0 files that triggers a compaction into L1.
	// Lower values reduce read amplification but increase write amplification.
	// Default: 4 (CockroachDB recommendation). Original was 1 (too aggressive).
	KVLevel0FileNumCompactionTrigger int

	// KVLevel0StopWritesTrigger (L0StopWritesThreshold) is the number of L0 files
	// that will stop writes until compaction reduces the count.
	// Default: 12 (CockroachDB recommendation). Original 24 was too high.
	KVLevel0StopWritesTrigger int

	// ---- LSM Tree Settings ----

	// KVMaxBytesForLevelBase (LBaseMaxBytes) is the maximum size of L1.
	// Pebble dynamically sizes lower levels based on this.
	// Default: 64MB (CockroachDB recommendation for smaller datasets).
	KVMaxBytesForLevelBase int64

	// KVTargetFileSizeBase is the target size for L0 SSTable files.
	// Default: 4MB (smaller files = faster compaction, more files).
	KVTargetFileSizeBase int64

	// KVTargetFileSizeMultiplier controls file size growth per level.
	// Default: 2 (each level has 2x larger files than previous).
	KVTargetFileSizeMultiplier int64

	// KVNumOfLevels is the number of levels in the LSM tree.
	// Default: 7 (standard for most workloads).
	KVNumOfLevels int

	// ---- Block Settings ----

	// KVBlockSize is the size of data blocks within SSTable files.
	// Larger blocks improve compression but increase read amplification.
	// Default: 32KB (CockroachDB uses 32KB, original 64KB was too large).
	KVBlockSize int

	// KVBlockRestartInterval is the number of keys between restart points
	// in a data block. Lower values speed up seeks but increase space usage.
	// Default: 16 (standard value).
	KVBlockRestartInterval int

	// KVIndexBlockSize is the target size for index blocks.
	// Default: 256KB (matches CockroachDB).
	KVIndexBlockSize int

	// ---- Bloom Filter ----

	// KVBloomFilterBitsPerKey is the number of bits per key for bloom filters.
	// Higher values reduce false positive rate but increase memory/disk usage.
	// Set to 0 to disable bloom filters.
	// Default: 10 (1% false positive rate, standard recommendation).
	KVBloomFilterBitsPerKey int

	// ---- File Settings ----

	// KVMaxOpenFiles is the maximum number of open file descriptors.
	// Default: 10000 (reduced from 102400 which was excessive).
	KVMaxOpenFiles int

	// KVMaxManifestFileSize is the maximum size of the MANIFEST file before
	// it's rotated.
	// Default: 128MB.
	KVMaxManifestFileSize int64

	// ---- Sync Settings ----

	// KVBytesPerSync is the number of bytes to write before syncing SSTable files.
	// Default: 512KB (CockroachDB recommendation).
	KVBytesPerSync int

	// KVWALBytesPerSync is the number of bytes to write before syncing WAL.
	// Default: 0 (sync on every write for durability). Set higher for performance.
	KVWALBytesPerSync int

	// ---- Compaction Settings ----

	// KVMaxConcurrentCompactions is the maximum number of concurrent compaction jobs.
	// Default: 3 (matches CockroachDB recommendation).
	KVMaxConcurrentCompactions int

	// ---- Compression Settings ----

	// KVCompression specifies the compression algorithm for SSTable blocks.
	// Default: CompressionSnappy (fast, good for Raft logs).
	KVCompression CompressionType
}

// DefaultPebbleDBConfig returns a configuration optimized for Raft log storage
// based on CockroachDB's production settings.
func DefaultPebbleDBConfig() *PebbleDBConfig {
	return &PebbleDBConfig{
		// Cache: 128MB is reasonable for moderate workloads
		KVLRUCacheSize: 128 * 1024 * 1024,

		// MemTable: 64MB per memtable (CockroachDB default)
		KVWriteBufferSize:      64 * 1024 * 1024,
		KVMaxWriteBufferNumber: 4,

		// L0 Compaction: CockroachDB defaults
		KVLevel0FileNumCompactionTrigger: 4,
		KVLevel0StopWritesTrigger:        12,

		// LSM Tree sizing
		KVMaxBytesForLevelBase:     64 * 1024 * 1024, // 64MB
		KVTargetFileSizeBase:       4 * 1024 * 1024,  // 4MB
		KVTargetFileSizeMultiplier: 2,
		KVNumOfLevels:              7,

		// Block settings (CockroachDB uses 32KB blocks)
		KVBlockSize:            32 * 1024, // 32KB
		KVBlockRestartInterval: 16,
		KVIndexBlockSize:       256 * 1024, // 256KB

		// Bloom filter: 10 bits/key gives ~1% false positive rate
		KVBloomFilterBitsPerKey: 10,

		// File descriptors
		KVMaxOpenFiles:        10000,
		KVMaxManifestFileSize: 128 * 1024 * 1024,

		// Sync settings
		KVBytesPerSync:    512 * 1024, // 512KB
		KVWALBytesPerSync: 0,          // Sync every write for durability

		// Compaction
		KVMaxConcurrentCompactions: 3,

		// Compression
		KVCompression: CompressionSnappy,
	}
}

// LowMemoryPebbleDBConfig returns a configuration optimized for
// memory-constrained environments.
func LowMemoryPebbleDBConfig() *PebbleDBConfig {
	cfg := DefaultPebbleDBConfig()
	cfg.KVLRUCacheSize = 32 * 1024 * 1024    // 32MB cache
	cfg.KVWriteBufferSize = 16 * 1024 * 1024 // 16MB memtable
	cfg.KVMaxWriteBufferNumber = 2           // Fewer memtables
	cfg.KVMaxBytesForLevelBase = 32 * 1024 * 1024
	cfg.KVMaxOpenFiles = 1000
	cfg.KVMaxConcurrentCompactions = 1
	return cfg
}

// HighPerformancePebbleDBConfig returns a configuration optimized for
// high-throughput workloads with more memory available.
func HighPerformancePebbleDBConfig() *PebbleDBConfig {
	cfg := DefaultPebbleDBConfig()
	cfg.KVLRUCacheSize = 512 * 1024 * 1024    // 512MB cache
	cfg.KVWriteBufferSize = 128 * 1024 * 1024 // 128MB memtable
	cfg.KVMaxWriteBufferNumber = 6
	cfg.KVMaxBytesForLevelBase = 128 * 1024 * 1024
	cfg.KVMaxConcurrentCompactions = 6
	cfg.KVBytesPerSync = 1024 * 1024   // 1MB
	cfg.KVWALBytesPerSync = 256 * 1024 // 256KB (less frequent WAL sync)
	return cfg
}

// toPebbleCompressionFunc converts CompressionType to a function returning *sstable.CompressionProfile.
// Pebble v2 uses CompressionProfile instead of simple compression constants.
func (c CompressionType) toPebbleCompressionFunc() func() *sstable.CompressionProfile {
	switch c {
	case CompressionSnappy:
		return func() *sstable.CompressionProfile { return sstable.SnappyCompression }
	case CompressionZstd:
		return func() *sstable.CompressionProfile { return sstable.ZstdCompression }
	case CompressionNone:
		return func() *sstable.CompressionProfile { return sstable.NoCompression }
	default:
		return func() *sstable.CompressionProfile { return sstable.SnappyCompression }
	}
}

// buildLevelOptions creates level-specific options based on the configuration.
func (cfg *PebbleDBConfig) buildLevelOptions() [7]pebble.LevelOptions {
	var lopts [7]pebble.LevelOptions

	compressionFunc := cfg.KVCompression.toPebbleCompressionFunc()

	for l := 0; l < cfg.KVNumOfLevels && l < 7; l++ {
		lopts[l] = pebble.LevelOptions{
			BlockSize:      cfg.KVBlockSize,
			IndexBlockSize: cfg.KVIndexBlockSize,
			// Use bloom filter for all levels to speed up point lookups
			FilterPolicy: bloom.FilterPolicy(cfg.KVBloomFilterBitsPerKey),
			// Compression function for Pebble v2
			Compression:          compressionFunc,
			BlockRestartInterval: cfg.KVBlockRestartInterval,
		}
	}

	return lopts
}

// buildTargetFileSizes creates per-level target file sizes.
func (cfg *PebbleDBConfig) buildTargetFileSizes() [7]int64 {
	var sizes [7]int64
	sz := cfg.KVTargetFileSizeBase

	for l := 0; l < cfg.KVNumOfLevels && l < 7; l++ {
		sizes[l] = sz
		sz = sz * cfg.KVTargetFileSizeMultiplier
	}

	return sizes
}
