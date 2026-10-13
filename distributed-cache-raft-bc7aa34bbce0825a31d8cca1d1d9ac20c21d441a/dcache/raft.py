from dataclasses import dataclass, field

@dataclass
class LogEntry:
    term: int
    command: dict

@dataclass
class RaftNode:
    node_id: str
    peers: list[str]
    current_term: int = 0
    voted_for: str | None = None
    role: str = "follower"
    leader_id: str | None = None
    log: list[LogEntry] = field(default_factory=list)
    commit_index: int = -1

    def become_candidate(self):
        self.current_term += 1
        self.role = "candidate"
        self.voted_for = self.node_id
        self.leader_id = None

    def become_leader(self):
        self.role = "leader"
        self.leader_id = self.node_id

    def become_follower(self, term, leader_id=None):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.role = "follower"
        self.leader_id = leader_id

    def append_command(self, command):
        if self.role != "leader":
            raise RuntimeError("not leader")
        self.log.append(LogEntry(self.current_term, command))
        return len(self.log) - 1

    @staticmethod
    def majority(total_nodes):
        return total_nodes // 2 + 1

    def can_commit(self, replicated_count, total_nodes):
        return replicated_count >= self.majority(total_nodes)
