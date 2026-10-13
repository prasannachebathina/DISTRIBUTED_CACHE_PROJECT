# Test plan

- Unit: hash ring, LRU, WAL recovery, Raft state.
- Concurrency: repeated parallel requests and race-sensitive tests.
- Persistence: kill/restart a node and recover WAL.
- Fault injection: stop one pod/process, delay/loss with Linux tc.
- Performance: throughput, p50, p95, p99.
- Comparison: run the same workload against RocksDB/etcd/TiKV and record actual values.
