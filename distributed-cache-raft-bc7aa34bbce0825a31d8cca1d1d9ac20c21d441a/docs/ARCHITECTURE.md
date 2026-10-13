# Architecture

Client
 -> Consistent Hash Ring
 -> Cache Node
 -> Raft replication/commit
 -> WAL
 -> Memtable
 -> SSTable

The academic implementation should extend the Raft core with real RequestVote,
AppendEntries, election timers, persistent term/vote state, log matching and
majority commit before the final production-grade claim.
