package raftpebbledb

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"go.uber.org/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/raft"
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("__logs__")
	// Upper bound for dbLogs iteration (next prefix after __logs__)
	dbLogsUpperBound = []byte("__logs__\xff")
	dbConf           = []byte("__conf__")
	def              = []byte("__def__")

	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

type PebbleStore struct {
	path   string
	logger pebble.Logger
	db     *pebble.DB

	closed *atomic.Bool
}

func NewPebbleStore(path string, logger pebble.Logger, cfg *PebbleDBConfig) (*PebbleStore, error) {
	if cfg == nil {
		cfg = DefaultPebbleDBConfig()
	}

	db, err := OpenPebbleDB(cfg, path, logger)
	if err != nil {
		return nil, err
	}

	ps := &PebbleStore{
		path:   path,
		logger: logger,
		db:     db,
		closed: atomic.NewBool(false),
	}

	return ps, nil
}

// FirstIndex returns the first index written. 0 for no entries.
func (ps *PebbleStore) FirstIndex() (uint64, error) {
	if ps.isclosed() {
		return 0, pebble.ErrClosed
	}

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: dbLogs,
		UpperBound: dbLogsUpperBound,
	})

	if err != nil {
		return 0, err
	}

	defer iter.Close()

	if !iter.First() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if !iter.Valid() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		return 0, nil
	}

	key := iter.Key()
	if len(key) == 0 {
		return 0, nil
	}

	return bytesToUint64(ps.dblogKey(key)), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (ps *PebbleStore) LastIndex() (uint64, error) {
	if ps.isclosed() {
		return 0, pebble.ErrClosed
	}

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: dbLogs,
		UpperBound: dbLogsUpperBound,
	})

	if err != nil {
		return 0, err
	}

	defer iter.Close()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		return 0, nil
	}

	if !iter.Valid() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		return 0, nil
	}

	key := iter.Key()
	if len(key) == 0 {
		return 0, nil
	}

	return bytesToUint64(ps.dblogKey(key)), nil
}

// GetLog gets a log entry at a given index.
func (ps *PebbleStore) GetLog(index uint64, log *raft.Log) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	key := ps.buildKey(dbLogs, uint64ToBytes(index))

	val, err := ps.getBytes(key)
	if err != nil {
		return err
	}

	if len(val) == 0 {
		return raft.ErrLogNotFound
	}

	return decodeMsgPack(val, log)
}

// StoreLog stores a log entry.
func (ps *PebbleStore) StoreLog(log *raft.Log) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.StoreLogs([]*raft.Log{log})
}

// StoreLogs stores multiple log entries. By default the logs stored may not be contiguous with previous logs (i.e. may have a gap in Index since the last log written). If an implementation can't tolerate this it may optionally implement `MonotonicLogStore` to indicate that this is not allowed. This changes Raft's behaviour after restoring a user snapshot to remove all previous logs instead of relying on a "gap" to signal the discontinuity between logs before the snapshot and logs after.
func (ps *PebbleStore) StoreLogs(logs []*raft.Log) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	batch := ps.db.NewBatch()
	defer batch.Close()

	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}

		if err := batch.Set(ps.buildKey(dbLogs, key), val.Bytes(), pebble.Sync); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// DeleteRange deletes a range of log entries, [min, max]. The range is inclusive.
func (ps *PebbleStore) DeleteRange(min, max uint64) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	minKey := uint64ToBytes(min)
	maxKey := uint64ToBytes(max + 1)

	return ps.db.DeleteRange(ps.buildKey(dbLogs, minKey), ps.buildKey(dbLogs, maxKey), pebble.Sync)

	// iter := ps.db.NewIter(&pebble.IterOptions{
	// 	LowerBound: ps.buildKey(dbLogs, minKey),
	// })
	// defer iter.Close()

	// if !iter.Valid() {
	// 	return 0, errors.New("NewIter returns an iterator that is unpositioned")
	// }

	// batch := ps.db.NewBatch()
	// defer batch.Close()

	// for iter.First(); iter.Valid(); iter.Next() {
	// 	key := iter.Key()

	// 	// Handle out-of-range log index
	// 	if bytesToUint64(ps.dblogKey(key)) > max {
	// 		break
	// 	}

	// 	batch.Delete(key, pebble.Sync)
	// }

	// return batch.Commit(pebble.Sync)
}

// Set is used to set a key/value set outside of the raft log
func (ps *PebbleStore) Set(key, val []byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.Set(ps.buildKey(dbConf, key), val, pebble.Sync)
}

// Get is used to retrieve a value from the k/v store by key
func (ps *PebbleStore) Get(key []byte) ([]byte, error) {
	if ps.isclosed() {
		return nil, pebble.ErrClosed
	}

	val, err := ps.getBytes(ps.buildKey(dbConf, key))
	if err != nil {
		return nil, err
	}

	if len(val) == 0 {
		return nil, ErrKeyNotFound
	}

	return val, nil
}

// SetUint64 is like Set, but handles uint64 values
func (ps *PebbleStore) SetUint64(key []byte, val uint64) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.Set(ps.buildKey(def, key), uint64ToBytes(val), pebble.Sync)
}

// GetUint64 is like Get, but handles uint64 values
func (ps *PebbleStore) GetUint64(key []byte) (uint64, error) {
	if ps.isclosed() {
		return 0, pebble.ErrClosed
	}

	val, err := ps.getBytes(ps.buildKey(def, key))
	if err != nil {
		return 0, err
	}

	if len(val) == 0 {
		return 0, nil
	}

	return bytesToUint64(val), nil
}

// Delete removes a key from the k/v store
func (ps *PebbleStore) Delete(key []byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.Delete(ps.buildKey(dbConf, key), pebble.Sync)
}

// Exists checks if a key exists in the k/v store without retrieving the value.
// This is more efficient than Get when you only need to check existence.
func (ps *PebbleStore) Exists(key []byte) (bool, error) {
	if ps.isclosed() {
		return false, pebble.ErrClosed
	}

	_, closer, err := ps.db.Get(ps.buildKey(dbConf, key))
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if err := closer.Close(); err != nil {
		return false, err
	}

	return true, nil
}

// SetBatch atomically sets multiple key-value pairs.
// This is more efficient than calling Set multiple times.
func (ps *PebbleStore) SetBatch(kvs map[string][]byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	batch := ps.db.NewBatch()
	defer batch.Close()

	for k, v := range kvs {
		if err := batch.Set(ps.buildKey(dbConf, []byte(k)), v, nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// DeleteBatch atomically deletes multiple keys.
// This is more efficient than calling Delete multiple times.
func (ps *PebbleStore) DeleteBatch(keys [][]byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	batch := ps.db.NewBatch()
	defer batch.Close()

	for _, key := range keys {
		if err := batch.Delete(ps.buildKey(dbConf, key), nil); err != nil {
			return err
		}
	}

	return batch.Commit(pebble.Sync)
}

// KVDeleteRange deletes all keys in the range [start, end).
// The range is inclusive of start and exclusive of end.
func (ps *PebbleStore) KVDeleteRange(start, end []byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.DeleteRange(
		ps.buildKey(dbConf, start),
		ps.buildKey(dbConf, end),
		pebble.Sync,
	)
}

// Scan iterates over all key-value pairs in the range [start, end) and calls
// the callback function for each pair. If the callback returns false, iteration stops.
// Pass nil for start to begin at the first key, and nil for end to iterate to the last key.
func (ps *PebbleStore) Scan(start, end []byte, fn func(key, value []byte) bool) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	lowerBound := dbConf
	upperBound := []byte("__conf__\xff") // upper bound for dbConf

	if start != nil {
		lowerBound = ps.buildKey(dbConf, start)
	}
	if end != nil {
		upperBound = ps.buildKey(dbConf, end)
	}

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Strip the dbConf prefix from the key
		userKey := key[len(dbConf):]

		// Copy the values since they're only valid until the next iteration
		keyCopy := make([]byte, len(userKey))
		copy(keyCopy, userKey)

		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		if !fn(keyCopy, valueCopy) {
			break
		}
	}

	return iter.Error()
}

// ScanPrefix iterates over all key-value pairs with the given prefix and calls
// the callback function for each pair. If the callback returns false, iteration stops.
func (ps *PebbleStore) ScanPrefix(prefix []byte, fn func(key, value []byte) bool) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	lowerBound := ps.buildKey(dbConf, prefix)
	// Create upper bound by incrementing the last byte of prefix
	upperBound := make([]byte, len(lowerBound))
	copy(upperBound, lowerBound)
	upperBound[len(upperBound)-1]++

	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Strip the dbConf prefix from the key
		userKey := key[len(dbConf):]

		// Copy the values since they're only valid until the next iteration
		keyCopy := make([]byte, len(userKey))
		copy(keyCopy, userKey)

		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		if !fn(keyCopy, valueCopy) {
			break
		}
	}

	return iter.Error()
}

// Checkpoint creates a point-in-time snapshot of the database at the given directory.
// The checkpoint can be opened as a read-only database for backup or inspection.
func (ps *PebbleStore) Checkpoint(destDir string) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.Checkpoint(destDir, pebble.WithFlushedWAL())
}

// Compact manually triggers compaction for the given key range [start, end].
// This can be useful for reclaiming space after many deletions.
// Pass nil for both start and end to compact the entire database.
func (ps *PebbleStore) Compact(start, end []byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	ctx := context.Background()
	return ps.db.Compact(ctx, start, end, true)
}

// Metrics returns the current database metrics including disk usage,
// compaction statistics, and cache hit rates.
func (ps *PebbleStore) Metrics() *pebble.Metrics {
	if ps.isclosed() {
		return nil
	}

	return ps.db.Metrics()
}

// DBPath returns the path to the database directory.
func (ps *PebbleStore) DBPath() string {
	return ps.path
}

func (ps *PebbleStore) buildKey(prefix, key []byte) []byte {
	// Create a new slice to avoid modifying the shared prefix backing array
	result := make([]byte, len(prefix)+len(key))
	copy(result, prefix)
	copy(result[len(prefix):], key)
	return result
}

func (ps *PebbleStore) dblogKey(key []byte) []byte {
	return key[len(dbLogs):]
}

func (ps *PebbleStore) getBytes(key []byte) ([]byte, error) {
	if ps.closed.Load() {
		return []byte{}, pebble.ErrClosed
	}

	val, closer, err := ps.db.Get(key)
	// 查询的key不存在，返回空值
	if err == pebble.ErrNotFound {
		return []byte{}, nil
	}

	if err != nil {
		return nil, err
	}

	// 这里需要copy
	data := make([]byte, len(val))
	copy(data, val)

	if err := closer.Close(); err != nil {
		return nil, err
	}

	return data, nil
}

func (ps *PebbleStore) isclosed() bool {
	return ps.closed.Load()
}

func (ps *PebbleStore) Close() error {
	if ps == nil {
		return nil
	}

	ps.closed.Store(true) // set pebbledb closed

	if ps.db != nil {
		if err := ps.db.Flush(); err != nil {
			// Continue with close even if flush fails, but capture the error
			closeErr := ps.db.Close()
			ps.db = nil
			if closeErr != nil {
				return closeErr
			}
			return err
		}
		if err := ps.db.Close(); err != nil {
			ps.db = nil
			return err
		}
		ps.db = nil
	}

	return nil
}

func (ps *PebbleStore) Sync() error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}
	return ps.db.Flush()
}

func OpenPebbleDB(cfg *PebbleDBConfig, dir string, logger pebble.Logger) (*pebble.DB, error) {
	dataPath := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walPath, os.ModePerm); err != nil {
		return nil, err
	}

	// Build level options with bloom filter, compression, and block settings
	lopts := cfg.buildLevelOptions()
	targetFileSizes := cfg.buildTargetFileSizes()

	cache := pebble.NewCache(cfg.KVLRUCacheSize)
	opts := &pebble.Options{
		// Sync settings
		BytesPerSync:    cfg.KVBytesPerSync,
		WALBytesPerSync: cfg.KVWALBytesPerSync,

		// Level configuration with bloom filter and compression
		Levels:          lopts,
		TargetFileSizes: targetFileSizes,

		// MemTable settings
		MemTableSize:                cfg.KVWriteBufferSize,
		MemTableStopWritesThreshold: cfg.KVMaxWriteBufferNumber,

		// LSM tree settings
		LBaseMaxBytes:         cfg.KVMaxBytesForLevelBase,
		L0CompactionThreshold: cfg.KVLevel0FileNumCompactionTrigger,
		L0StopWritesThreshold: cfg.KVLevel0StopWritesTrigger,

		// File settings
		MaxManifestFileSize: cfg.KVMaxManifestFileSize,
		MaxOpenFiles:        cfg.KVMaxOpenFiles,

		// Cache and WAL
		Cache:  cache,
		WALDir: walPath,
		Logger: logger,

		// Use the newest format for best performance and features
		FormatMajorVersion: pebble.FormatNewest,

		// Compaction concurrency (min, max)
		CompactionConcurrencyRange: func() (int, int) {
			cc := cfg.KVMaxConcurrentCompactions
			// Allow some dynamic scaling: min is 1, max is configured value
			if cc <= 1 {
				return 1, 1
			}
			return 1, cc
		},
	}

	event := &eventListener{
		log: logger,
	}

	opts.EventListener = &pebble.EventListener{
		BackgroundError:  event.BackgroundError,
		CompactionBegin:  event.CompactionBegin,
		CompactionEnd:    event.CompactionEnd,
		DiskSlow:         event.DiskSlow,
		FlushBegin:       event.FlushBegin,
		FlushEnd:         event.FlushEnd,
		ManifestCreated:  event.ManifestCreated,
		ManifestDeleted:  event.ManifestDeleted,
		TableCreated:     event.TableCreated,
		TableDeleted:     event.TableDeleted,
		TableIngested:    event.TableIngested,
		TableStatsLoaded: event.TableStatsLoaded,
		WALCreated:       event.WALCreated,
		WALDeleted:       event.WALDeleted,
		WriteStallBegin:  event.WriteStallBegin,
		WriteStallEnd:    event.WriteStallEnd,
	}

	db, err := pebble.Open(dataPath, opts)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	cache.Unref()

	return db, nil
}
