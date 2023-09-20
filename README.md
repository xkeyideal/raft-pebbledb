raft-pebbledb
===========

This implementation uses the maintained version of [PebbleDB](https://github.com/cockroachdb/pebble). This is the primary version of `raft-pebbledb` and should be used whenever possible. 

There is no breaking API change to the library. However, there is the potential for disk format incompatibilities so it was decided to be conservative and making it a separate import path.

Cautions:

1. `raft-pebbledb` write kv datas, use `pebble.Sync` WriteOptions which synchronize to disk.
2. if use `pebble.NoSync` WriteOptions which do not synchronize to disk, maybe lost data when the program crashed suddenly.
3. if we call `Flush()` before exit process for flush datas to disk, use `pebble.NoSync` WriteOptions will not be lost datas.

## Benchmark

PebbleDB(NoSync)

```
goos: darwin
goarch: amd64
pkg: github.com/xkeyideal/raft-pebbledb
cpu: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
BenchmarkPebbleStore_FirstIndex-8        2176722               515.1 ns/op
BenchmarkPebbleStore_LastIndex-8         1788973               632.8 ns/op
BenchmarkPebbleStore_GetLog-8             548466              2165 ns/op
BenchmarkPebbleStore_StoreLog-8              194           5865119 ns/op
BenchmarkPebbleStore_StoreLogs-8             194           5840854 ns/op
BenchmarkPebbleStore_DeleteRange-8        345982              5076 ns/op
BenchmarkPebbleStore_Set-8                   196           5832603 ns/op
BenchmarkPebbleStore_Get-8               3414891               341.0 ns/op
BenchmarkPebbleStore_SetUint64-8             186           5953613 ns/op
BenchmarkPebbleStore_GetUint64-8         3385880               367.1 ns/op
PASS
ok      github.com/xkeyideal/raft-pebbledb      30.014s
```

PebbleDB(Sync)

```
goos: darwin
goarch: amd64
pkg: github.com/xkeyideal/raft-pebbledb
cpu: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
BenchmarkPebbleStore_FirstIndex-8        1977333               587.7 ns/op
BenchmarkPebbleStore_LastIndex-8         1832170               706.3 ns/op
BenchmarkPebbleStore_GetLog-8             349820              2972 ns/op
BenchmarkPebbleStore_StoreLog-8              219           5293872 ns/op
BenchmarkPebbleStore_StoreLogs-8             223           5189428 ns/op
BenchmarkPebbleStore_DeleteRange-8           207           6649486 ns/op
BenchmarkPebbleStore_Set-8                   214           5460250 ns/op
BenchmarkPebbleStore_Get-8               2909061               365.2 ns/op
BenchmarkPebbleStore_SetUint64-8             214           5297888 ns/op
BenchmarkPebbleStore_GetUint64-8         3130579               380.3 ns/op
PASS
ok      github.com/xkeyideal/raft-pebbledb      22.785s
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