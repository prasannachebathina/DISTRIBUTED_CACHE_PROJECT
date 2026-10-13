import bisect
import hashlib

class ConsistentHashRing:
    def __init__(self, replicas=100):
        self.replicas = replicas
        self._ring = {}
        self._points = []

    @staticmethod
    def _hash(value):
        return int(hashlib.sha256(value.encode()).hexdigest(), 16)

    def add_node(self, node_id):
        for i in range(self.replicas):
            self._ring[self._hash(f"{node_id}#{i}")] = node_id
        self._points = sorted(self._ring)

    def remove_node(self, node_id):
        for i in range(self.replicas):
            self._ring.pop(self._hash(f"{node_id}#{i}"), None)
        self._points = sorted(self._ring)

    def get_node(self, key):
        if not self._points:
            raise RuntimeError("empty ring")
        h = self._hash(key)
        i = bisect.bisect_left(self._points, h)
        if i == len(self._points):
            i = 0
        return self._ring[self._points[i]]

    def nodes(self):
        return sorted(set(self._ring.values()))
