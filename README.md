raft-pebbledb
===========

This implementation uses [PebbleDB v2.x](https://github.com/cockroachdb/pebble), a LevelDB/RocksDB inspired key-value store focused on performance and internal usage by CockroachDB.

## Version Information

**Current Pebble Version**: v2.1.4

This library uses Pebble v2.x, which includes significant improvements and optimizations over v1.x. Please note the following important compatibility considerations:

### Format Compatibility Warning

⚠️ **Pebble v2.x does not support the oldest on-disk formats from v1.x**

If you are upgrading from a previous version of this library that used Pebble v1.x:
- **New deployments**: No action needed - v2.x will work out of the box
- **Existing deployments with data**: You are responsible for migrating your database format before upgrading
  - See [Pebble's Format Major Versions documentation](https://github.com/cockroachdb/pebble#format-major-versions) for migration guidance
  - Format migration is a one-way operation and cannot be reversed
  - Test the migration process in a non-production environment first

### API Stability

The raft-pebbledb library API remains stable. This upgrade only affects:
- Internal Pebble dependency (import path changes to `github.com/cockroachdb/pebble/v2`)
- Database disk format compatibility (as noted above)
- No breaking changes to raft-pebbledb's public API

## Usage

Cautions:

1. `raft-pebbledb` write kv datas, use `pebble.Sync` WriteOptions which synchronize to disk.
2. if use `pebble.NoSync` WriteOptions which do not synchronize to disk, maybe lost data when the program crashed suddenly.
3. if we call `Flush()` before exit process for flush datas to disk, use `pebble.NoSync` WriteOptions will not be lost datas.

## Examples

### KV-only example

This repo includes a minimal KV-only example at `examples/kv`.
It demonstrates `Set/Get` and `SetUint64/GetUint64`, then closes and reopens the store to verify persistence.

Run with a temporary directory (auto-cleanup):

```bash
go run ./examples/kv -cleanup
```

Run with a specified directory (so you can inspect `data/` and `wal/`):

```bash
go run ./examples/kv -dir ./tmp/raftpebbledb-demo
```

## Benchmark

Benchmarks performed with Pebble v2.1.4 on Apple M4 (2026):

### PebbleDB with Sync

```
goos: darwin
goarch: arm64
pkg: github.com/xkeyideal/raft-pebbledb
cpu: Apple M4
BenchmarkPebbleStore_FirstIndex-10       7217354               477.1 ns/op             0 B/op          0 allocs/op
BenchmarkPebbleStore_LastIndex-10        6966156               499.7 ns/op            80 B/op          1 allocs/op
BenchmarkPebbleStore_GetLog-10           3441195               996.4 ns/op          1218 B/op         35 allocs/op
BenchmarkPebbleStore_StoreLog-10             801           3989189 ns/op            2473 B/op         28 allocs/op
BenchmarkPebbleStore_StoreLogs-10            836           3844470 ns/op            5333 B/op         76 allocs/op
BenchmarkPebbleStore_DeleteRange-10          878           3865595 ns/op             840 B/op          5 allocs/op
BenchmarkPebbleStore_Set-10                  915           3833800 ns/op             667 B/op          4 allocs/op
BenchmarkPebbleStore_Get-10             11729859               282.0 ns/op            20 B/op          3 allocs/op
BenchmarkPebbleStore_SetUint64-10            931           3773065 ns/op             542 B/op          3 allocs/op
BenchmarkPebbleStore_GetUint64-10       11227593               288.3 ns/op            32 B/op          3 allocs/op
Benchmark_PebbleSync_Single-10               820           3841499 ns/op            3911 B/op          5 allocs/op
Benchmark_PebbleSync_Batch-10             360706              9372 ns/op            6575 B/op          4 allocs/op
PASS
ok      github.com/xkeyideal/raft-pebbledb      63.176s
```

### PebbleDB with NoSync

```
goos: darwin
goarch: arm64
pkg: github.com/xkeyideal/raft-pebbledb
cpu: Apple M4
Benchmark_PebbleNoSync_Single-10          414559             12162 ns/op            2542 B/op          4 allocs/op
Benchmark_PebbleNoSync_Batch-10           323400             17346 ns/op            6999 B/op          4 allocs/op
PASS
```

[BoltDB](https://github.com/hashicorp/raft-boltdb)

```
goos: darwin
goarch: amd64
pkg: github.com/hashicorp/raft-boltdb/v2
cpu: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
BenchmarkBoltStore_FirstIndex-8          2647128               454.2 ns/op
BenchmarkBoltStore_LastIndex-8           2587760               491.4 ns/op
BenchmarkBoltStore_GetLog-8               568754              1919 ns/op
BenchmarkBoltStore_StoreLog-8                102          16080469 ns/op
BenchmarkBoltStore_StoreLogs-8               100          11640950 ns/op
BenchmarkBoltStore_DeleteRange-8             103          11172144 ns/op
BenchmarkBoltStore_Set-8                     100          11548708 ns/op
BenchmarkBoltStore_Get-8                 1944616               594.2 ns/op
BenchmarkBoltStore_SetUint64-8               104          11446542 ns/op
BenchmarkBoltStore_GetUint64-8           1973713               606.2 ns/op
PASS
ok      github.com/hashicorp/raft-boltdb/v2     21.202s
```