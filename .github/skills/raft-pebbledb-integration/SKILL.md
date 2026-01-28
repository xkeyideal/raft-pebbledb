---
name: raft-pebbledb-integration
description: Integrate raft-pebbledb (Pebble-based Hashicorp Raft LogStore + StableStore) into a Raft node.
license: BSD-2-Clause
compatibility: Go modules; Hashicorp Raft; requires a pebble.Logger implementation.
metadata:
  author: xkeyideal
  version: "1.0"
---

将 `github.com/xkeyideal/raft-pebbledb/v2` 作为 Hashicorp Raft 的持久化存储接入：它同时实现 `raft.LogStore` 与 `raft.StableStore`，底层使用 PebbleDB（`github.com/cockroachdb/pebble/v2`）。

**适用场景**
- 你正在使用 `github.com/hashicorp/raft` 构建 Raft 节点，需要一个高性能、可持久化的 log/stable store。
- 你希望将 raft 的日志与 stable KV 落盘（默认使用 `pebble.Sync`）。

**重要提示（升级/格式）**
- 该库基于 Pebble v2.x；如果你从 Pebble v1.x 迁移并且已有存量数据，需要自行处理 Pebble 格式迁移（不可逆）。

**输入**
- 用户应提供：Raft 节点的数据目录（如 `./data/raft`）、以及现有初始化 Raft 的代码位置（如果不知道，先搜索 `raft.NewRaft` / `raft.NewRaft(`）。

**步骤**

1. **在接入方项目中添加依赖**
   - 运行：
     ```bash
     go get github.com/xkeyideal/raft-pebbledb/v2@latest
     go mod tidy
     ```

2. **准备一个 `pebble.Logger` 实现（最小可用）**
   - 如果项目已有 logger，推荐适配到 `pebble.Logger`（需要 `Infof/Errorf/Fatalf`）。
   - 若没有，使用一个最小实现：
     ```go
     type pebbleLogger struct{}
     func (l *pebbleLogger) Infof(string, ...any)  {}
     func (l *pebbleLogger) Errorf(string, ...any) {}
     func (l *pebbleLogger) Fatalf(format string, args ...any) {
         panic(fmt.Sprintf(format, args...))
     }
     ```

3. **创建 PebbleStore 并接入 Raft**
   - 最常见的接入方式：用同一个 `*raftpebbledb.PebbleStore` 同时作为 `LogStore` 和 `StableStore`：
     ```go
     // 使用默认配置（基于 CockroachDB 生产调优）
     store, err := raftpebbledb.NewPebbleStore(raftDir, &pebbleLogger{}, nil)
     if err != nil { return err }
     defer store.Close()

     logStore := raft.LogStore(store)
     stableStore := raft.StableStore(store)

     // snapStore / transport / fsm / config 仍按你现有方式创建
     r, err := raft.NewRaft(cfg, fsm, logStore, stableStore, snapStore, transport)
     if err != nil { return err }
     ```
   - **配置选项**：提供三种预置配置：
     ```go
     // 默认配置（推荐大多数场景）
     cfg := raftpebbledb.DefaultPebbleDBConfig()
     
     // 内存受限环境（32MB 缓存，16MB memtable）
     cfg := raftpebbledb.LowMemoryPebbleDBConfig()
     
     // 高性能环境（512MB 缓存，128MB memtable）
     cfg := raftpebbledb.HighPerformancePebbleDBConfig()
     
     store, err := raftpebbledb.NewPebbleStore(raftDir, logger, cfg)
     ```
   - **压缩选项**：
     ```go
     cfg := raftpebbledb.DefaultPebbleDBConfig()
     cfg.KVCompression = raftpebbledb.CompressionZstd // 更高压缩率
     // 或 CompressionSnappy（默认，更快）
     // 或 CompressionNone（无压缩）
     ```

4. **落盘与退出策略**
   - 该库对 raft log / stable KV 写入默认使用 `pebble.Sync`（更安全，吞吐会低一些）。
   - 退出时确保调用 `Close()`；它会 `Flush()` 并 `Close()` DB。
   - 如你需要显式 flush，可调用 `Sync()`（内部执行 `db.Flush()`）。

5. **扩展 KV 操作**
   除了 Raft 接口，该库还提供以下扩展 KV 方法：
   ```go
   // 单键操作
   store.Delete(key)                  // 删除单个键
   store.Exists(key)                  // 检查键是否存在（不获取值，更高效）
   
   // 批量操作（原子性，更高效）
   store.SetBatch(map[string][]byte{"k1": v1, "k2": v2})
   store.DeleteBatch([][]byte{key1, key2, key3})
   store.KVDeleteRange(start, end)    // 删除范围 [start, end)
   
   // 范围扫描
   store.Scan(start, end, func(k, v []byte) bool { return true })
   store.ScanPrefix(prefix, func(k, v []byte) bool { return true })
   
   // 运维操作
   store.Checkpoint(destDir)          // 创建数据库快照（备份）
   store.Compact(start, end)          // 手动触发压缩
   store.Metrics()                    // 获取数据库指标
   store.DBPath()                     // 获取数据库路径
   ```

6. **路径与权限**
   - `NewPebbleStore(path, ...)` 会在 `path/` 下创建子目录：`data/` 与 `wal/`。
   - 确保进程对该目录具备可写权限，并把它放在持久化磁盘上。

7. **最小验证**
   - 在接入方项目中：
     ```bash
     go test ./...
     ```
   - 或在你的启动流程中做一次 smoke test：创建 store → `Set/Get` 一对 key → 关闭 → 重启读取。

**输出（完成标准）**
- `go.mod` 已包含 `github.com/xkeyideal/raft-pebbledb/v2` 依赖。
- Raft 初始化代码已改为使用 `raftpebbledb.NewPebbleStore`，并正确传入 `LogStore` 与 `StableStore`。
- shutdown 路径中确保执行 `store.Close()`。

**故障排查**
- `pebble.ErrClosed`：通常是 store 已关闭仍在使用；检查生命周期（defer/退出流程）。
- `raft.ErrLogNotFound`：表示对应 index 的 raft log 不存在（可能被 snapshot/compact 删除）。
- `pebble: empty table`：Pebble 可能在 flush 的边界情况下返回该错误，通常不影响正确性；本库已将其在事件回调中降级为 info 级别。
- 存量数据升级后无法打开：检查是否存在 Pebble v1 -> v2 格式不兼容问题，按 Pebble 文档进行迁移。

**仅 KV 最小示例（可直接运行）**
- 仓库内置了一个仅 KV 的示例程序：`examples/kv`，会执行 `Set/Get`、`SetUint64/GetUint64`，并通过关闭后重开验证落盘。
- 运行：
   ```bash
   go run ./examples/kv -cleanup
   ```
- 指定数据目录（便于你观察 `data/` 与 `wal/`）：
   ```bash
   go run ./examples/kv -dir ./tmp/raftpebbledb-demo
   ```

**Guardrails**
- 不要在接入方项目中引入额外的 DB 层封装；优先最小改动完成接入。
- 不改变 Raft 的 snapStore / transport / fsm 逻辑，除非用户明确要求。
- 避免把日志写入行为改成 `pebble.NoSync`，除非用户接受崩溃丢数据风险并有 flush/退出策略。
