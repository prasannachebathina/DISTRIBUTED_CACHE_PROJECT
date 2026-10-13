from .raft import RaftNode, LogEntry
from concurrent import futures
import threading
import grpc
import time

from prometheus_client import start_http_server

from .storage import LSMStorage
from .lru import LRUCache
from .raft import RaftNode
from .metrics import REQUESTS, LATENCY, HITS, MISSES

from proto import cache_pb2, cache_pb2_grpc


class CacheNode(
    cache_pb2_grpc.CacheServiceServicer,
    cache_pb2_grpc.RaftServiceServicer,
    cache_pb2_grpc.RecoveryServiceServicer
):

    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers

        self.storage = LSMStorage(f"data/{node_id}")
        self.cache = LRUCache(1000)

        self.raft = RaftNode(node_id, peers)

        self.lock = threading.RLock()

    # ---------------- GET ----------------
    
    def Get(self, request, context):
        with LATENCY.labels("get").time():
            REQUESTS.labels("get").inc()

            with self.lock:
                value = self.cache.get(request.key)

                if value is None:
                    value = self.storage.get(request.key)

                    if value is not None:
                        self.cache.set(request.key, value)

                if value is None:
                    MISSES.inc()

                    return cache_pb2.GetResponse(
                        found=False,
                        node_id=self.node_id
                    )

                HITS.inc()

                return cache_pb2.GetResponse(
                    found=True,
                    value=value,
                    node_id=self.node_id
                )

    # ---------------- RECOVERY LOG ----------------

    def GetLog(self, request, context):
        with self.lock:
            entries = []

            for entry in self.raft.log:
                op = entry.command.get("op")

                if op == "set":
                    entries.append(
                        f"set {entry.command['key']} {entry.command['value']}"
                    )

                elif op == "delete":
                    entries.append(
                        f"delete {entry.command['key']}"
                    )

            return cache_pb2.GetLogResponse(
                term=self.raft.current_term,
                entries=entries,
                commit_index=self.raft.commit_index
            )

    # ---------------- REPLICATION ----------------

    def replicate_to_peers(self, command=None):
        success_count = 1

        entries = []

        for entry in self.raft.log:
            op = entry.command.get("op")

            if op == "set":
                entries.append(
                    f"set {entry.command['key']} {entry.command['value']}"
                )

            elif op == "delete":
                entries.append(
                    f"delete {entry.command['key']}"
                )

        for peer in self.peers:
            try:
                port = {
                    "node1": 50051,
                    "node2": 50052,
                    "node3": 50053
                }.get(peer)

                if not port:
                    continue

                peer_host = {
                    "node1": "node1",
                    "node2": "node2",
                    "node3": "node3",
                }.get(peer, "localhost")

                channel = grpc.insecure_channel(f"{peer_host}:{port}")

                stub = cache_pb2_grpc.RaftServiceStub(channel)

                response = stub.AppendEntries(
                    cache_pb2.AppendEntriesRequest(
                        term=self.raft.current_term,
                        leader_id=self.node_id,
                        prev_log_index=-1,
                        prev_log_term=0,
                        entries=entries,
                        leader_commit=self.raft.commit_index
                    ),
                    timeout=2
                )

                if response.success:
                    success_count += 1

                channel.close()

            except Exception:
                pass

        return success_count

    # ---------------- SET ----------------
    def recover_from_peer(self):
        peer_ports = {
            "node1": 50051,
            "node2": 50052,
            "node3": 50053,
        }

        for peer in self.peers:
            try:
                port = peer_ports.get(peer)
                if not port:
                    continue
                peer_host = {
                "node1": "node1",
                "node2": "node2",
                "node3": "node3",
                }.get(peer, "localhost")

                channel = grpc.insecure_channel(f"{peer_host}:{port}")
                stub = cache_pb2_grpc.RecoveryServiceStub(channel)

                response = stub.GetLog(
                    cache_pb2.GetLogRequest(
                        term=self.raft.current_term,
                        node_id=self.node_id
                    ),
                    timeout=2
                )

                for entry in response.entries:
                    parts = entry.split(" ", 2)

                    if parts[0] == "set" and len(parts) == 3:
                        key = parts[1]
                        value = parts[2]

                        self.storage.set(key, value)
                        self.cache.set(key, value)

                        self.raft.log.append(
                            LogEntry(
                                self.raft.current_term,
                                {
                                    "op": "set",
                                    "key": key,
                                    "value": value
                                }
                            )
                        )

                    elif parts[0] == "delete" and len(parts) >= 2:
                        key = parts[1]

                        self.storage.delete(key)
                        self.cache.delete(key)

                        self.raft.log.append(
                            LogEntry(
                                self.raft.current_term,
                                {
                                    "op": "delete",
                                    "key": key
                                }
                            )
                        )

                self.raft.current_term = max(
                    self.raft.current_term,
                    response.term
                )

                self.raft.commit_index = min(
                    response.commit_index,
                    len(self.raft.log) - 1
                )

                channel.close()
                return True

            except Exception:
                continue

        return False
    def Set(self, request, context):
        with LATENCY.labels("set").time():
            REQUESTS.labels("set").inc()

            with self.lock:

                if self.raft.role != "leader":
                    self.raft.become_leader()

                idx = self.raft.append_command({
                    "op": "set",
                    "key": request.key,
                    "value": request.value,
                    "ttl_seconds": request.ttl_seconds
                })

                replicated_count = self.replicate_to_peers()

                if not self.raft.can_commit(
                    replicated_count,
                    len(self.peers) + 1
                ):
                    return cache_pb2.SetResponse(
                        ok=False,
                        node_id=self.node_id,
                        leader_id=self.raft.leader_id or self.node_id
                    )

                self.raft.commit_index = idx

                self.storage.set(
                    request.key,
                    request.value,
                    request.ttl_seconds
                )

                self.cache.set(
                    request.key,
                    request.value
                )

                return cache_pb2.SetResponse(
                    ok=True,
                    node_id=self.node_id,
                    leader_id=self.raft.leader_id or self.node_id,
                    commit_index=idx
                )

    # ---------------- DELETE ----------------

    def Delete(self, request, context):
        with LATENCY.labels("delete").time():
            REQUESTS.labels("delete").inc()

            with self.lock:

                if self.raft.role != "leader":
                    self.raft.become_leader()

                idx = self.raft.append_command({
                    "op": "delete",
                    "key": request.key
                })

                replicated_count = self.replicate_to_peers()

                if not self.raft.can_commit(
                    replicated_count,
                    len(self.peers) + 1
                ):
                    return cache_pb2.DeleteResponse(
                        ok=False,
                        node_id=self.node_id,
                        leader_id=self.raft.leader_id or self.node_id
                    )

                self.raft.commit_index = idx

                self.storage.delete(request.key)
                self.cache.delete(request.key)

                return cache_pb2.DeleteResponse(
                    ok=True,
                    node_id=self.node_id,
                    leader_id=self.raft.leader_id or self.node_id,
                    commit_index=idx
                )

    # ---------------- REQUEST VOTE ----------------

    def RequestVote(self, request, context):
        with self.lock:

            if request.term < self.raft.current_term:
                return cache_pb2.RequestVoteResponse(
                    term=self.raft.current_term,
                    vote_granted=False
                )

            if request.term > self.raft.current_term:
                self.raft.become_follower(
                    request.term
                )

            can_vote = (
                self.raft.voted_for is None
                or self.raft.voted_for == request.candidate_id
            )

            if can_vote:
                self.raft.voted_for = request.candidate_id

                return cache_pb2.RequestVoteResponse(
                    term=self.raft.current_term,
                    vote_granted=True
                )

            return cache_pb2.RequestVoteResponse(
                term=self.raft.current_term,
                vote_granted=False
            )

    # ---------------- APPEND ENTRIES ----------------

    def AppendEntries(self, request, context):
        with self.lock:

            if request.term < self.raft.current_term:
                return cache_pb2.AppendEntriesResponse(
                    term=self.raft.current_term,
                    success=False,
                    match_index=len(self.raft.log) - 1
                )

            if request.term > self.raft.current_term:
                self.raft.become_follower(
                    request.term,
                    request.leader_id
                )
            else:
                self.raft.role = "follower"
                self.raft.leader_id = request.leader_id

            start_index = len(self.raft.log)

            for entry in request.entries[start_index:]:

                parts = entry.split(" ", 2)

                if parts[0] == "set" and len(parts) == 3:

                    key = parts[1]
                    value = parts[2]

                    from .raft import LogEntry

                    self.raft.log.append(
                        LogEntry(
                            self.raft.current_term,
                            {
                                "op": "set",
                                "key": key,
                                "value": value,
                                "ttl_seconds": 0
                            }
                        )
                    )

                    self.storage.set(
                        key,
                        value
                    )

                    self.cache.set(
                        key,
                        value
                    )

                elif parts[0] == "delete" and len(parts) >= 2:

                    key = parts[1]

                    from .raft import LogEntry

                    self.raft.log.append(
                        LogEntry(
                            self.raft.current_term,
                            {
                                "op": "delete",
                                "key": key
                            }
                        )
                    )

                    self.storage.delete(key)
                    self.cache.delete(key)

            if request.leader_commit > self.raft.commit_index:
                self.raft.commit_index = min(
                    request.leader_commit,
                    len(self.raft.log) - 1
                )

            return cache_pb2.AppendEntriesResponse(
                term=self.raft.current_term,
                success=True,
                match_index=len(self.raft.log) - 1
            )


# ---------------- SERVER ----------------

def serve(node_id, port, peers, metrics_port):

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=32)
    )

    cache_node = CacheNode(
        node_id,
        port,
        peers
    )

    cache_pb2_grpc.add_CacheServiceServicer_to_server(
        cache_node,
        server
    )

    cache_pb2_grpc.add_RaftServiceServicer_to_server(
        cache_node,
        server
    )

    cache_pb2_grpc.add_RecoveryServiceServicer_to_server(
        cache_node,
        server
    )

    server.add_insecure_port(
        f"0.0.0.0:{port}"
    )

    start_http_server(
        metrics_port
    )

    server.start()

    print(
        f"{node_id} listening on {port}"
    )
    time.sleep(2)
    cache_node.recover_from_peer()

    server.wait_for_termination()
