package raftpebbledb

import (
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble"
	raftbench "github.com/hashicorp/raft/bench"
)

func BenchmarkPebbleStore_FirstIndex(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkPebbleStore_LastIndex(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkPebbleStore_GetLog(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkPebbleStore_StoreLog(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkPebbleStore_StoreLogs(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkPebbleStore_DeleteRange(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkPebbleStore_Set(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkPebbleStore_Get(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkPebbleStore_SetUint64(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkPebbleStore_GetUint64(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}

func newPebbledb() *PebbleStore {
	dir := filepath.Join("/Users/xkey/test/", "pebble-sync-test")
	// os.RemoveAll(dir)

	store, err := NewPebbleStore(dir, &Logger{}, DefaultPebbleDBConfig())
	if err != nil {
		panic(err)
	}

	// write 100w data in pebbledb first
	// for i := 0; i < 1000000; i++ {
	// 	key := randomId(32)
	// 	val := randomId(1024)
	// 	err := store.db.Set(key, val, pebble.Sync)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }

	return store
}

// go test -v -benchmem -run=^$ -bench ^Benchmark_PebbleSync_Single$
func Benchmark_PebbleSync_Single(b *testing.B) {
	store := newPebbledb()
	defer store.Close()

	keys := [][]byte{}
	for n := 0; n < b.N; n++ {
		key := randomId(32)
		val := randomId(1024)
		if err := store.db.Set(key, val, pebble.Sync); err != nil {
			b.Fatalf("err: %s", err)
		}

		if n < 10 {
			keys = append(keys, key)
		}
	}

	for _, key := range keys {
		val, closer, err := store.db.Get(key)
		if err != nil {
			panic(err)
		}

		// 这里需要copy
		data := make([]byte, len(val))
		copy(data, val)

		if err := closer.Close(); err != nil {
			panic(err)
		}

		// b.Log(string(key), string(data))
	}
}

// go test -v -benchmem -run=^$ -bench ^Benchmark_PebbleSync_Batch$
func Benchmark_PebbleSync_Batch(b *testing.B) {
	store := newPebbledb()
	defer store.Close()

	keys := [][]byte{}
	batch := store.db.NewBatch()
	defer batch.Close()

	for n := 0; n < b.N; n++ {
		key := randomId(32)
		val := randomId(1024)
		if err := batch.Set(key, val, pebble.Sync); err != nil {
			b.Fatalf("err: %s", err)
		}

		if n < 10 {
			keys = append(keys, key)
		}
	}

	err := batch.Commit(pebble.Sync)
	if err != nil {
		panic(err)
	}

	for _, key := range keys {
		val, closer, err := store.db.Get(key)
		if err != nil {
			panic(err)
		}

		// 这里需要copy
		data := make([]byte, len(val))
		copy(data, val)

		if err := closer.Close(); err != nil {
			panic(err)
		}

		// b.Log(string(key), string(data))
	}
}

// go test -v -benchmem -run=^$ -bench ^Benchmark_PebbleNoSync_Single$
func Benchmark_PebbleNoSync_Single(b *testing.B) {
	store := newPebbledb()
	defer store.Close()

	keys := [][]byte{}
	for n := 0; n < b.N; n++ {
		key := randomId(32)
		val := randomId(1024)
		if err := store.db.Set(key, val, pebble.NoSync); err != nil {
			b.Fatalf("err: %s", err)
		}

		if n < 10 {
			keys = append(keys, key)
		}
	}

	store.db.Flush()

	for _, key := range keys {
		val, closer, err := store.db.Get(key)
		if err != nil {
			panic(err)
		}

		// 这里需要copy
		data := make([]byte, len(val))
		copy(data, val)

		if err := closer.Close(); err != nil {
			panic(err)
		}

		// b.Log(string(key), string(data))
	}
}

// go test -v -benchmem -run=^$ -bench ^Benchmark_PebbleNoSync_Batch$
func Benchmark_PebbleNoSync_Batch(b *testing.B) {
	store := newPebbledb()
	defer store.Close()

	keys := [][]byte{}
	batch := store.db.NewBatch()
	defer batch.Close()

	for n := 0; n < b.N; n++ {
		key := randomId(32)
		val := randomId(1024)
		if err := batch.Set(key, val, pebble.NoSync); err != nil {
			b.Fatalf("err: %s", err)
		}

		if n < 10 {
			keys = append(keys, key)
		}
	}

	err := batch.Commit(pebble.NoSync)
	if err != nil {
		panic(err)
	}

	// store.db.Flush()

	for _, key := range keys {
		val, closer, err := store.db.Get(key)
		if err != nil {
			panic(err)
		}

		// 这里需要copy
		data := make([]byte, len(val))
		copy(data, val)

		if err := closer.Close(); err != nil {
			panic(err)
		}

		// b.Log(string(key), string(data))
	}
}

var idChars = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")

func randomId(idLen int) []byte {
	b := randomBytesMod(idLen, byte(len(idChars)))
	for i, c := range b {
		b[i] = idChars[c]
	}
	return b
}

func randomBytes(length int) (b []byte) {
	b = make([]byte, length)
	io.ReadFull(rand.Reader, b)
	return
}

func randomBytesMod(length int, mod byte) (b []byte) {
	maxrb := 255 - byte(256%int(mod))
	b = make([]byte, length)
	i := 0
	for {
		r := randomBytes(length + (length / 4))
		for _, c := range r {
			if c > maxrb {
				continue
			}
			b[i] = c % mod
			i++
			if i == length {
				return b
			}
		}
	}
}
