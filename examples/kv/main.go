package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	raftpebbledb "github.com/xkeyideal/raft-pebbledb/v2"
)

type pebbleLogger struct{}

func (l *pebbleLogger) Infof(string, ...any)              {}
func (l *pebbleLogger) Errorf(format string, args ...any) { fmt.Printf(format, args...) }
func (l *pebbleLogger) Fatalf(format string, args ...any) { panic(fmt.Sprintf(format, args...)) }

func main() {
	var dir string
	var cleanup bool
	flag.StringVar(&dir, "dir", "", "data directory for pebble store (will create data/ and wal/ under it)")
	flag.BoolVar(&cleanup, "cleanup", false, "remove dir after run")
	flag.Parse()

	if dir == "" {
		dir = filepath.Join(os.TempDir(), "raft-pebbledb-kv-example-"+time.Now().Format("20060102-150405"))
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		panic(err)
	}
	if cleanup {
		defer os.RemoveAll(dir)
	}

	logger := &pebbleLogger{}

	fmt.Printf("using dir: %s\n", dir)

	store, err := raftpebbledb.NewPebbleStore(dir, logger, nil)
	if err != nil {
		panic(err)
	}

	key := []byte("hello")
	value := []byte("world")
	if err := store.Set(key, value); err != nil {
		panic(err)
	}

	got, err := store.Get(key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Get(%q) = %q\n", key, got)

	u64Key := []byte("counter")
	if err := store.SetUint64(u64Key, 42); err != nil {
		panic(err)
	}
	gotU64, err := store.GetUint64(u64Key)
	if err != nil {
		panic(err)
	}
	fmt.Printf("GetUint64(%q) = %d\n", u64Key, gotU64)

	if err := store.Close(); err != nil {
		panic(err)
	}

	store2, err := raftpebbledb.NewPebbleStore(dir, logger, nil)
	if err != nil {
		panic(err)
	}
	defer store2.Close()

	got2, err := store2.Get(key)
	if err != nil {
		panic(err)
	}
	gotU642, err := store2.GetUint64(u64Key)
	if err != nil {
		panic(err)
	}

	fmt.Printf("reopen: Get(%q) = %q\n", key, got2)
	fmt.Printf("reopen: GetUint64(%q) = %d\n", u64Key, gotU642)
}
