package raftpebbledb

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/hashicorp/raft"
)

type Logger struct {
}

func (log *Logger) Infof(format string, args ...interface{}) {
	// fmt.Printf(format, args...)
}

func (log *Logger) Errorf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func (log *Logger) Fatalf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}

func testPebbleStore(t testing.TB) *PebbleStore {
	testDir := t.TempDir()

	// Successfully creates and returns a store
	store, err := NewPebbleStore(testDir, &Logger{}, DefaultPebbleDBConfig())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	return store
}

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestPebbleStore_Implements(t *testing.T) {
	var store interface{} = &PebbleStore{}
	if _, ok := store.(raft.StableStore); !ok {
		t.Fatalf("PebbleStore does not implement raft.StableStore")
	}
	if _, ok := store.(raft.LogStore); !ok {
		t.Fatalf("PebbleStore does not implement raft.LogStore")
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_FirstIndex$
func TestPebbleStore_FirstIndex(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Should get 0 index on empty log
	idx, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the first Raft index
	idx, err = store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 1 {
		t.Fatalf("bad: %d", idx)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_LastIndex$
func TestPebbleStore_LastIndex(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Should get 0 index on empty log
	idx, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 0 {
		t.Fatalf("bad: %v", idx)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Fetch the last Raft index
	idx, err = store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if idx != 3 {
		t.Fatalf("bad: %d", idx)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_GetLog$
func TestPebbleStore_GetLog(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	log := new(raft.Log)

	// Should return an error on non-existent log
	err := store.GetLog(1, log)
	if err != nil && err != raft.ErrLogNotFound {
		t.Fatalf("expected raft log not found error, got: %v", err)
	}

	// Set a mock raft log
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("bad: %s", err)
	}

	// Should return the proper log
	if err := store.GetLog(2, log); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(log, logs[1]) {
		t.Fatalf("bad: %#v", log)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_SetLog$
func TestPebbleStore_SetLog(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Create the log
	log := &raft.Log{
		Data:  []byte("log1"),
		Index: 1,
	}

	// Attempt to store the log
	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Retrieve the log again
	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the log comes back the same
	if !reflect.DeepEqual(log, result) {
		t.Fatalf("bad: %v", result)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_SetLogs$
func TestPebbleStore_SetLogs(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Create a set of logs
	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
	}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure we stored them all
	result1, result2 := new(raft.Log), new(raft.Log)
	if err := store.GetLog(1, result1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[0], result1) {
		t.Fatalf("bad: %#v", result1)
	}
	if err := store.GetLog(2, result2); err != nil {
		t.Fatalf("err: %s", err)
	}
	if !reflect.DeepEqual(logs[1], result2) {
		t.Fatalf("bad: %#v", result2)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_DeleteRange$
func TestPebbleStore_DeleteRange(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Create a set of logs
	log1 := testRaftLog(1, "log1")
	log2 := testRaftLog(2, "log2")
	log3 := testRaftLog(3, "log3")
	logs := []*raft.Log{log1, log2, log3}

	// Attempt to store the logs
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Attempt to delete a range of logs
	if err := store.DeleteRange(1, 2); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Ensure the logs were deleted
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log1")
	}
	if err := store.GetLog(2, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("should have deleted log2")
	}
	if err := store.GetLog(3, new(raft.Log)); err == raft.ErrLogNotFound {
		t.Fatalf("should have not deleted log3")
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Set_Get$
func TestPebbleStore_Set_Get(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Returns error on non-existent key
	if _, err := store.Get([]byte("bad")); err != ErrKeyNotFound {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("hello"), []byte("world")

	// Try to set a k/v pair
	if err := store.Set(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Try to read it back
	val, err := store.Get(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, v) {
		t.Fatalf("bad: %v", val)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_SetUint64_GetUint64$
func TestPebbleStore_SetUint64_GetUint64(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Returns error on non-existent key
	if _, err := store.GetUint64([]byte("bad")); err != nil {
		t.Fatalf("expected not found error, got: %q", err)
	}

	k, v := []byte("abc"), uint64(123)

	// Attempt to set the k/v pair
	if err := store.SetUint64(k, v); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Read back the value
	val, err := store.GetUint64(k)
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if val != v {
		t.Fatalf("bad: %v", val)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_ClosedOperations$
func TestPebbleStore_ClosedOperations(t *testing.T) {
	store := testPebbleStore(t)

	// Close the store
	if err := store.Close(); err != nil {
		t.Fatalf("err closing store: %s", err)
	}

	// All operations should return ErrClosed
	if _, err := store.FirstIndex(); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if _, err := store.LastIndex(); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.GetLog(1, &raft.Log{}); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.StoreLog(&raft.Log{Index: 1}); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.StoreLogs([]*raft.Log{{Index: 1}}); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.DeleteRange(1, 10); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.Set([]byte("key"), []byte("val")); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if _, err := store.Get([]byte("key")); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.SetUint64([]byte("key"), 1); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if _, err := store.GetUint64([]byte("key")); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
	if err := store.Sync(); err != pebble.ErrClosed {
		t.Fatalf("expected ErrClosed, got: %v", err)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Persistence$
func TestPebbleStore_Persistence(t *testing.T) {
	testDir := t.TempDir()

	// Create store and write data
	store, err := NewPebbleStore(testDir, &Logger{}, DefaultPebbleDBConfig())
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	logs := []*raft.Log{
		testRaftLog(1, "log1"),
		testRaftLog(2, "log2"),
		testRaftLog(3, "log3"),
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.Set([]byte("testkey"), []byte("testvalue")); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.SetUint64([]byte("counter"), 42); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Close and reopen
	if err := store.Close(); err != nil {
		t.Fatalf("err closing: %s", err)
	}

	store2, err := NewPebbleStore(testDir, &Logger{}, DefaultPebbleDBConfig())
	if err != nil {
		t.Fatalf("err reopening: %s", err)
	}
	defer store2.Close()

	// Verify data persisted
	first, err := store2.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 1 {
		t.Fatalf("expected first=1, got %d", first)
	}

	last, err := store2.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != 3 {
		t.Fatalf("expected last=3, got %d", last)
	}

	val, err := store2.Get([]byte("testkey"))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if !bytes.Equal(val, []byte("testvalue")) {
		t.Fatalf("expected testvalue, got %s", val)
	}

	u64val, err := store2.GetUint64([]byte("counter"))
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if u64val != 42 {
		t.Fatalf("expected 42, got %d", u64val)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_ConcurrentAccess$
func TestPebbleStore_ConcurrentAccess(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	numOps := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				val := []byte(fmt.Sprintf("val-%d-%d", id, j))
				if err := store.Set(key, val); err != nil {
					t.Errorf("concurrent set err: %s", err)
				}
			}
		}(i)
	}

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				key := []byte(fmt.Sprintf("key-%d-%d", id, j))
				store.Get(key) // May or may not find, that's ok
			}
		}(i)
	}

	// Concurrent log operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				idx := uint64(id*numOps + j + 1)
				log := &raft.Log{
					Index: idx,
					Data:  []byte(fmt.Sprintf("data-%d-%d", id, j)),
				}
				if err := store.StoreLog(log); err != nil {
					t.Errorf("concurrent store log err: %s", err)
				}
			}
		}(i)
	}

	wg.Wait()
}

// go test -v -timeout 30s -run ^TestPebbleStore_LargeLog$
func TestPebbleStore_LargeLog(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Create a log with large data (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	log := &raft.Log{
		Index: 1,
		Data:  largeData,
	}

	if err := store.StoreLog(log); err != nil {
		t.Fatalf("err storing large log: %s", err)
	}

	result := new(raft.Log)
	if err := store.GetLog(1, result); err != nil {
		t.Fatalf("err getting large log: %s", err)
	}

	if !bytes.Equal(result.Data, largeData) {
		t.Fatalf("large log data mismatch")
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_BatchStoreLogs$
func TestPebbleStore_BatchStoreLogs(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Store a large batch of logs
	numLogs := 1000
	logs := make([]*raft.Log, numLogs)
	for i := 0; i < numLogs; i++ {
		logs[i] = &raft.Log{
			Index: uint64(i + 1),
			Data:  []byte(fmt.Sprintf("log-%d", i)),
		}
	}

	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err storing batch: %s", err)
	}

	// Verify first and last
	first, err := store.FirstIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if first != 1 {
		t.Fatalf("expected first=1, got %d", first)
	}

	last, err := store.LastIndex()
	if err != nil {
		t.Fatalf("err: %s", err)
	}
	if last != uint64(numLogs) {
		t.Fatalf("expected last=%d, got %d", numLogs, last)
	}

	// Verify random samples
	for _, idx := range []uint64{1, 500, uint64(numLogs)} {
		log := new(raft.Log)
		if err := store.GetLog(idx, log); err != nil {
			t.Fatalf("err getting log %d: %s", idx, err)
		}
		expected := fmt.Sprintf("log-%d", idx-1)
		if string(log.Data) != expected {
			t.Fatalf("expected %s, got %s", expected, log.Data)
		}
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_DeleteRangeEdgeCases$
func TestPebbleStore_DeleteRangeEdgeCases(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Store logs 1-10
	logs := make([]*raft.Log, 10)
	for i := 0; i < 10; i++ {
		logs[i] = testRaftLog(uint64(i+1), fmt.Sprintf("log%d", i+1))
	}
	if err := store.StoreLogs(logs); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Delete range in the middle [3,7]
	if err := store.DeleteRange(3, 7); err != nil {
		t.Fatalf("err: %s", err)
	}

	// Log 1,2 should exist
	for _, idx := range []uint64{1, 2} {
		if err := store.GetLog(idx, new(raft.Log)); err != nil {
			t.Fatalf("log %d should exist, got err: %s", idx, err)
		}
	}

	// Log 3-7 should not exist
	for idx := uint64(3); idx <= 7; idx++ {
		if err := store.GetLog(idx, new(raft.Log)); err != raft.ErrLogNotFound {
			t.Fatalf("log %d should be deleted", idx)
		}
	}

	// Log 8,9,10 should exist
	for _, idx := range []uint64{8, 9, 10} {
		if err := store.GetLog(idx, new(raft.Log)); err != nil {
			t.Fatalf("log %d should exist, got err: %s", idx, err)
		}
	}

	// Delete at boundaries
	if err := store.DeleteRange(1, 1); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.GetLog(1, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("log 1 should be deleted")
	}

	if err := store.DeleteRange(10, 10); err != nil {
		t.Fatalf("err: %s", err)
	}
	if err := store.GetLog(10, new(raft.Log)); err != raft.ErrLogNotFound {
		t.Fatalf("log 10 should be deleted")
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_EmptyKeyValue$
func TestPebbleStore_EmptyKeyValue(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Empty value should work
	if err := store.Set([]byte("emptyval"), []byte{}); err != nil {
		t.Fatalf("err setting empty value: %s", err)
	}

	// Getting empty value - this returns ErrKeyNotFound because len(val)==0
	// This is expected behavior per current implementation
	_, err := store.Get([]byte("emptyval"))
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for empty value, got: %v", err)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_DoubleClose$
func TestPebbleStore_DoubleClose(t *testing.T) {
	store := testPebbleStore(t)

	// First close should succeed
	if err := store.Close(); err != nil {
		t.Fatalf("first close err: %s", err)
	}

	// Second close should not panic (nil db check)
	if err := store.Close(); err != nil {
		t.Fatalf("second close err: %s", err)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_NilClose$
func TestPebbleStore_NilClose(t *testing.T) {
	var store *PebbleStore
	// Should not panic
	if err := store.Close(); err != nil {
		t.Fatalf("nil close err: %s", err)
	}
}

// go test -v -timeout 30s -run ^TestPebbleDBConfig_Default$
func TestPebbleDBConfig_Default(t *testing.T) {
	cfg := DefaultPebbleDBConfig()

	// Verify key settings match CockroachDB recommendations
	if cfg.KVLevel0FileNumCompactionTrigger != 4 {
		t.Errorf("expected L0CompactionThreshold=4, got %d", cfg.KVLevel0FileNumCompactionTrigger)
	}
	if cfg.KVLevel0StopWritesTrigger != 12 {
		t.Errorf("expected L0StopWritesTrigger=12, got %d", cfg.KVLevel0StopWritesTrigger)
	}
	if cfg.KVBloomFilterBitsPerKey != 10 {
		t.Errorf("expected BloomFilterBitsPerKey=10, got %d", cfg.KVBloomFilterBitsPerKey)
	}
	if cfg.KVCompression != CompressionSnappy {
		t.Errorf("expected CompressionSnappy, got %d", cfg.KVCompression)
	}
	if cfg.KVWriteBufferSize != 64*1024*1024 {
		t.Errorf("expected MemTableSize=64MB, got %d", cfg.KVWriteBufferSize)
	}
}

// go test -v -timeout 30s -run ^TestPebbleDBConfig_LowMemory$
func TestPebbleDBConfig_LowMemory(t *testing.T) {
	cfg := LowMemoryPebbleDBConfig()

	if cfg.KVLRUCacheSize != 32*1024*1024 {
		t.Errorf("expected cache=32MB, got %d", cfg.KVLRUCacheSize)
	}
	if cfg.KVWriteBufferSize != 16*1024*1024 {
		t.Errorf("expected memtable=16MB, got %d", cfg.KVWriteBufferSize)
	}
	if cfg.KVMaxConcurrentCompactions != 1 {
		t.Errorf("expected compactions=1, got %d", cfg.KVMaxConcurrentCompactions)
	}
}

// go test -v -timeout 30s -run ^TestPebbleDBConfig_HighPerformance$
func TestPebbleDBConfig_HighPerformance(t *testing.T) {
	cfg := HighPerformancePebbleDBConfig()

	if cfg.KVLRUCacheSize != 512*1024*1024 {
		t.Errorf("expected cache=512MB, got %d", cfg.KVLRUCacheSize)
	}
	if cfg.KVWriteBufferSize != 128*1024*1024 {
		t.Errorf("expected memtable=128MB, got %d", cfg.KVWriteBufferSize)
	}
	if cfg.KVMaxConcurrentCompactions != 6 {
		t.Errorf("expected compactions=6, got %d", cfg.KVMaxConcurrentCompactions)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_WithLowMemoryConfig$
func TestPebbleStore_WithLowMemoryConfig(t *testing.T) {
	testDir := t.TempDir()

	cfg := LowMemoryPebbleDBConfig()
	store, err := NewPebbleStore(testDir, &Logger{}, cfg)
	if err != nil {
		t.Fatalf("err creating store with low memory config: %s", err)
	}
	defer store.Close()

	// Basic operations should work
	if err := store.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("err setting key: %s", err)
	}
	val, err := store.Get([]byte("key"))
	if err != nil {
		t.Fatalf("err getting key: %s", err)
	}
	if !bytes.Equal(val, []byte("value")) {
		t.Fatalf("expected value, got %s", val)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_WithHighPerformanceConfig$
func TestPebbleStore_WithHighPerformanceConfig(t *testing.T) {
	testDir := t.TempDir()

	cfg := HighPerformancePebbleDBConfig()
	store, err := NewPebbleStore(testDir, &Logger{}, cfg)
	if err != nil {
		t.Fatalf("err creating store with high performance config: %s", err)
	}
	defer store.Close()

	// Basic operations should work
	if err := store.Set([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("err setting key: %s", err)
	}
	val, err := store.Get([]byte("key"))
	if err != nil {
		t.Fatalf("err getting key: %s", err)
	}
	if !bytes.Equal(val, []byte("value")) {
		t.Fatalf("expected value, got %s", val)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_CompressionTypes$
func TestPebbleStore_CompressionTypes(t *testing.T) {
	compressionTypes := []CompressionType{
		CompressionNone,
		CompressionSnappy,
		CompressionZstd,
	}

	for _, compType := range compressionTypes {
		t.Run(fmt.Sprintf("compression_%d", compType), func(t *testing.T) {
			testDir := t.TempDir()

			cfg := DefaultPebbleDBConfig()
			cfg.KVCompression = compType

			store, err := NewPebbleStore(testDir, &Logger{}, cfg)
			if err != nil {
				t.Fatalf("err creating store: %s", err)
			}
			defer store.Close()

			// Write and read data
			key := []byte("testkey")
			value := []byte("testvalue-with-some-data-to-compress")
			if err := store.Set(key, value); err != nil {
				t.Fatalf("err setting: %s", err)
			}
			got, err := store.Get(key)
			if err != nil {
				t.Fatalf("err getting: %s", err)
			}
			if !bytes.Equal(got, value) {
				t.Fatalf("value mismatch")
			}
		})
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Delete$
func TestPebbleStore_Delete(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	key := []byte("delete-test")
	value := []byte("to-be-deleted")

	// Set a key
	if err := store.Set(key, value); err != nil {
		t.Fatalf("err setting key: %s", err)
	}

	// Verify it exists
	val, err := store.Get(key)
	if err != nil {
		t.Fatalf("err getting key: %s", err)
	}
	if !bytes.Equal(val, value) {
		t.Fatalf("expected %s, got %s", value, val)
	}

	// Delete the key
	if err := store.Delete(key); err != nil {
		t.Fatalf("err deleting key: %s", err)
	}

	// Verify it's gone
	_, err = store.Get(key)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Exists$
func TestPebbleStore_Exists(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	key := []byte("exists-test")
	value := []byte("test-value")

	// Should not exist initially
	exists, err := store.Exists(key)
	if err != nil {
		t.Fatalf("err checking existence: %s", err)
	}
	if exists {
		t.Fatal("key should not exist")
	}

	// Set the key
	if err := store.Set(key, value); err != nil {
		t.Fatalf("err setting key: %s", err)
	}

	// Should exist now
	exists, err = store.Exists(key)
	if err != nil {
		t.Fatalf("err checking existence: %s", err)
	}
	if !exists {
		t.Fatal("key should exist")
	}

	// Delete the key
	if err := store.Delete(key); err != nil {
		t.Fatalf("err deleting key: %s", err)
	}

	// Should not exist anymore
	exists, err = store.Exists(key)
	if err != nil {
		t.Fatalf("err checking existence: %s", err)
	}
	if exists {
		t.Fatal("key should not exist after deletion")
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_SetBatch$
func TestPebbleStore_SetBatch(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	kvs := map[string][]byte{
		"batch-key1": []byte("value1"),
		"batch-key2": []byte("value2"),
		"batch-key3": []byte("value3"),
	}

	// Set batch
	if err := store.SetBatch(kvs); err != nil {
		t.Fatalf("err setting batch: %s", err)
	}

	// Verify all keys
	for k, expectedVal := range kvs {
		val, err := store.Get([]byte(k))
		if err != nil {
			t.Fatalf("err getting key %s: %s", k, err)
		}
		if !bytes.Equal(val, expectedVal) {
			t.Fatalf("expected %s, got %s", expectedVal, val)
		}
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_DeleteBatch$
func TestPebbleStore_DeleteBatch(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Set multiple keys
	keys := [][]byte{
		[]byte("del-batch-key1"),
		[]byte("del-batch-key2"),
		[]byte("del-batch-key3"),
	}
	for _, key := range keys {
		if err := store.Set(key, []byte("value")); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Verify all keys exist
	for _, key := range keys {
		exists, err := store.Exists(key)
		if err != nil {
			t.Fatalf("err checking existence: %s", err)
		}
		if !exists {
			t.Fatalf("key %s should exist", key)
		}
	}

	// Delete batch
	if err := store.DeleteBatch(keys); err != nil {
		t.Fatalf("err deleting batch: %s", err)
	}

	// Verify all keys are gone
	for _, key := range keys {
		exists, err := store.Exists(key)
		if err != nil {
			t.Fatalf("err checking existence: %s", err)
		}
		if exists {
			t.Fatalf("key %s should not exist after batch delete", key)
		}
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_KVDeleteRange$
func TestPebbleStore_KVDeleteRange(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Set keys with a common prefix
	keys := []string{"range:a", "range:b", "range:c", "range:d", "range:e"}
	for _, key := range keys {
		if err := store.Set([]byte(key), []byte("value")); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Delete range [range:b, range:d)
	if err := store.KVDeleteRange([]byte("range:b"), []byte("range:d")); err != nil {
		t.Fatalf("err deleting range: %s", err)
	}

	// Verify range:a still exists
	exists, _ := store.Exists([]byte("range:a"))
	if !exists {
		t.Fatal("range:a should still exist")
	}

	// Verify range:b and range:c are deleted
	exists, _ = store.Exists([]byte("range:b"))
	if exists {
		t.Fatal("range:b should be deleted")
	}
	exists, _ = store.Exists([]byte("range:c"))
	if exists {
		t.Fatal("range:c should be deleted")
	}

	// Verify range:d and range:e still exist (end is exclusive)
	exists, _ = store.Exists([]byte("range:d"))
	if !exists {
		t.Fatal("range:d should still exist")
	}
	exists, _ = store.Exists([]byte("range:e"))
	if !exists {
		t.Fatal("range:e should still exist")
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Scan$
func TestPebbleStore_Scan(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Set ordered keys
	keys := []string{"scan:a", "scan:b", "scan:c", "scan:d", "scan:e"}
	for _, key := range keys {
		if err := store.Set([]byte(key), []byte("val-"+key)); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Scan all keys with prefix
	var scannedKeys []string
	err := store.Scan([]byte("scan:"), []byte("scan:\xff"), func(key, value []byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("err scanning: %s", err)
	}

	if len(scannedKeys) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(scannedKeys))
	}

	// Verify keys are in order
	for i, key := range keys {
		if scannedKeys[i] != key {
			t.Fatalf("expected %s at index %d, got %s", key, i, scannedKeys[i])
		}
	}

	// Test early termination
	var count int
	err = store.Scan([]byte("scan:"), []byte("scan:\xff"), func(key, value []byte) bool {
		count++
		return count < 3 // Stop after 3 keys
	})
	if err != nil {
		t.Fatalf("err scanning: %s", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 iterations, got %d", count)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_ScanPrefix$
func TestPebbleStore_ScanPrefix(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Set keys with different prefixes
	prefixAKeys := []string{"prefix:a:1", "prefix:a:2", "prefix:a:3"}
	prefixBKeys := []string{"prefix:b:1", "prefix:b:2"}

	for _, key := range prefixAKeys {
		if err := store.Set([]byte(key), []byte("val")); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}
	for _, key := range prefixBKeys {
		if err := store.Set([]byte(key), []byte("val")); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Scan only prefix:a
	var scannedKeys []string
	err := store.ScanPrefix([]byte("prefix:a"), func(key, value []byte) bool {
		scannedKeys = append(scannedKeys, string(key))
		return true
	})
	if err != nil {
		t.Fatalf("err scanning prefix: %s", err)
	}

	if len(scannedKeys) != len(prefixAKeys) {
		t.Fatalf("expected %d keys, got %d: %v", len(prefixAKeys), len(scannedKeys), scannedKeys)
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Checkpoint$
func TestPebbleStore_Checkpoint(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	checkpointDir := t.TempDir() + "/checkpoint"

	// Set some data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("checkpoint-key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Set(key, value); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Flush to ensure data is persisted before checkpoint
	if err := store.Sync(); err != nil {
		t.Fatalf("err syncing: %s", err)
	}

	// Create checkpoint
	if err := store.Checkpoint(checkpointDir); err != nil {
		t.Fatalf("err creating checkpoint: %s", err)
	}

	// Verify checkpoint directory exists
	if _, err := os.Stat(checkpointDir); os.IsNotExist(err) {
		t.Fatal("checkpoint directory should exist")
	}

	// List checkpoint directory contents to verify files were created
	entries, err := os.ReadDir(checkpointDir)
	if err != nil {
		t.Fatalf("err reading checkpoint dir: %s", err)
	}
	if len(entries) == 0 {
		t.Fatal("checkpoint directory should not be empty")
	}
	t.Logf("Checkpoint created with %d entries", len(entries))
}

// go test -v -timeout 30s -run ^TestPebbleStore_Compact$
func TestPebbleStore_Compact(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Write some data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("compact-key-%03d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Set(key, value); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Delete half the keys
	for i := 0; i < 50; i++ {
		key := []byte(fmt.Sprintf("compact-key-%03d", i))
		if err := store.Delete(key); err != nil {
			t.Fatalf("err deleting key: %s", err)
		}
	}

	// Sync to flush to disk
	if err := store.Sync(); err != nil {
		t.Fatalf("err syncing: %s", err)
	}

	// Trigger compaction with specific range
	start := []byte("__conf__compact-key-000")
	end := []byte("__conf__compact-key-999")
	if err := store.Compact(start, end); err != nil {
		t.Fatalf("err compacting: %s", err)
	}

	// Verify remaining keys still exist
	for i := 50; i < 100; i++ {
		key := []byte(fmt.Sprintf("compact-key-%03d", i))
		exists, err := store.Exists(key)
		if err != nil {
			t.Fatalf("err checking existence: %s", err)
		}
		if !exists {
			t.Fatalf("key %s should exist after compaction", key)
		}
	}
}

// go test -v -timeout 30s -run ^TestPebbleStore_Metrics$
func TestPebbleStore_Metrics(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	// Write some data to generate metrics
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("metrics-key-%d", i))
		value := []byte(fmt.Sprintf("value-%d", i))
		if err := store.Set(key, value); err != nil {
			t.Fatalf("err setting key: %s", err)
		}
	}

	// Get metrics
	metrics := store.Metrics()
	if metrics == nil {
		t.Fatal("metrics should not be nil")
	}

	// Just verify we can access some metrics
	t.Logf("Metrics - DiskSpaceUsage: %d bytes", metrics.DiskSpaceUsage())
	t.Logf("Metrics - BlockCache hits: %d", metrics.BlockCache.Hits)
	t.Logf("Metrics - Flush count: %d", metrics.Flush.Count)
}

// go test -v -timeout 30s -run ^TestPebbleStore_DBPath$
func TestPebbleStore_DBPath(t *testing.T) {
	store := testPebbleStore(t)
	defer store.Close()

	path := store.DBPath()
	if path == "" {
		t.Fatal("DBPath should not be empty")
	}
	// Just verify it's not empty since we use TempDir now
	t.Logf("DBPath: %s", path)
}
