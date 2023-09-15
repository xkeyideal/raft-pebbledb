package raftpebbledb

import (
	"os"
	"path/filepath"

	"go.uber.org/atomic"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

var (
	// Bucket names we perform transactions in
	dbLogs = []byte("__logs__")
	dbConf = []byte("__conf__")
	def    = []byte("__def__")
)

type PebbleStore struct {
	path   string
	logger pebble.Logger
	db     *pebble.DB

	wo     *pebble.WriteOptions
	syncwo *pebble.WriteOptions

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

		wo:     &pebble.WriteOptions{Sync: false},
		syncwo: &pebble.WriteOptions{Sync: true},
		closed: atomic.NewBool(false),
	}

	return ps, nil
}

// FirstIndex returns the first index written. 0 for no entries.
func (ps *PebbleStore) FirstIndex() (uint64, error) {
	if ps.isclosed() {
		return 0, pebble.ErrClosed
	}

	iter, err := ps.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if !iter.First() {
		return 0, nil
	}

	return bytesToUint64(ps.dblogKey(iter.Key())), nil
}

// LastIndex returns the last index written. 0 for no entries.
func (ps *PebbleStore) LastIndex() (uint64, error) {
	if ps.isclosed() {
		return 0, pebble.ErrClosed
	}

	iter, err := ps.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	if !iter.Last() {
		return 0, nil
	}

	return bytesToUint64(ps.dblogKey(iter.Key())), nil
}

// GetLog gets a log entry at a given index.
func (ps *PebbleStore) GetLog(index uint64, log *raft.Log) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	key := ps.buildKey(dbLogs, uint64ToBytes(index))

	val, err := ps.getBytes(key)
	if err == pebble.ErrNotFound {
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

		if err := batch.Set(ps.buildKey(dbLogs, key), val.Bytes(), ps.wo); err != nil {
			return err
		}
	}

	return batch.Commit(ps.syncwo)
}

// DeleteRange deletes a range of log entries, [min, max]. The range is inclusive.
func (ps *PebbleStore) DeleteRange(min, max uint64) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	minKey := uint64ToBytes(min)
	maxKey := uint64ToBytes(max + 1)

	return ps.db.DeleteRange(ps.buildKey(dbLogs, minKey), ps.buildKey(dbLogs, maxKey), ps.wo)

	// iter, err := ps.db.NewIter(&pebble.IterOptions{
	// 	LowerBound: ps.buildKey(dbLogs, minKey),
	// })
	// if err != nil {
	// 	return err
	// }
	// defer iter.Close()

	// batch := ps.db.NewBatch()
	// defer batch.Close()

	// for iter.First(); iter.Valid(); iter.Next() {
	// 	key := iter.Key()

	// 	// Handle out-of-range log index
	// 	if bytesToUint64(ps.dblogKey(key)) > max {
	// 		break
	// 	}

	// 	batch.Delete(key, ps.wo)
	// }

	// return batch.Commit(ps.syncwo)
}

// Set is used to set a key/value set outside of the raft log
func (ps *PebbleStore) Set(key, val []byte) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.Set(ps.buildKey(dbConf, key), val, ps.syncwo)
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

	return val, nil
}

// SetUint64 is like Set, but handles uint64 values
func (ps *PebbleStore) SetUint64(key []byte, val uint64) error {
	if ps.isclosed() {
		return pebble.ErrClosed
	}

	return ps.db.Set(ps.buildKey(def, key), uint64ToBytes(val), ps.syncwo)
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

	return bytesToUint64(val), nil
}

func (ps *PebbleStore) buildKey(prefix, key []byte) []byte {
	return append(prefix, key...)
}

func (ps *PebbleStore) dblogKey(key []byte) []byte {
	return key[len(dbLogs):]
}

func (ps *PebbleStore) getBytes(key []byte) ([]byte, error) {
	if ps.closed.Load() {
		return []byte{}, pebble.ErrClosed
	}

	val, closer, err := ps.db.Get(key)
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
		ps.db.Flush()
		ps.db.Close()
		ps.db = nil
	}

	return nil
}

func (ps *PebbleStore) Sync() error {
	return ps.db.Flush()
}

func OpenPebbleDB(cfg *PebbleDBConfig, dir string, logger pebble.Logger) (*pebble.DB, error) {
	blockSize := cfg.KVBlockSize
	levelSizeMultiplier := cfg.KVTargetFileSizeMultiplier
	sz := cfg.KVTargetFileSizeBase
	lopts := make([]pebble.LevelOptions, 0)

	for l := 0; l < cfg.KVNumOfLevels; l++ {
		opt := pebble.LevelOptions{
			Compression:    pebble.DefaultCompression,
			BlockSize:      blockSize,
			TargetFileSize: sz,
		}
		sz = sz * levelSizeMultiplier
		lopts = append(lopts, opt)
	}

	dataPath := filepath.Join(dir, "data")
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dir, "wal")
	if err := os.MkdirAll(walPath, os.ModePerm); err != nil {
		return nil, err
	}

	cache := pebble.NewCache(cfg.KVLRUCacheSize)
	opts := &pebble.Options{
		BytesPerSync:                cfg.KVBytesPerSync,
		Levels:                      lopts,
		MaxManifestFileSize:         cfg.KVMaxManifestFileSize,
		MemTableSize:                cfg.KVWriteBufferSize,
		MemTableStopWritesThreshold: cfg.KVMaxWriteBufferNumber,
		LBaseMaxBytes:               cfg.KVMaxBytesForLevelBase,
		L0CompactionThreshold:       cfg.KVLevel0FileNumCompactionTrigger,
		L0StopWritesThreshold:       cfg.KVLevel0StopWritesTrigger,
		Cache:                       cache,
		WALDir:                      walPath,
		Logger:                      logger,
		MaxOpenFiles:                cfg.KVMaxOpenFiles,
		MaxConcurrentCompactions:    func() int { return cfg.KVMaxConcurrentCompactions },
		WALBytesPerSync:             cfg.KVWALBytesPerSync,
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
		return nil, err
	}
	cache.Unref()

	return db, nil
}
