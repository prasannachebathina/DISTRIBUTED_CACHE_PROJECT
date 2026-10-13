import json
import os
import time
from dataclasses import dataclass

@dataclass
class Entry:
    value: str
    expires_at: float | None = None

class LSMStorage:
    def __init__(self, data_dir, flush_threshold=100):
        self.data_dir = data_dir
        self.flush_threshold = flush_threshold
        os.makedirs(data_dir, exist_ok=True)
        self.wal_path = os.path.join(data_dir, "wal.log")
        self.sst_path = os.path.join(data_dir, "data.sst")
        self.memtable = {}
        self._recover()

    def _append_wal(self, record):
        with open(self.wal_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
            os.fsync(f.fileno())

    def _recover(self):
        if not os.path.exists(self.wal_path):
            return
        with open(self.wal_path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                if r["op"] == "set":
                    self.memtable[r["key"]] = Entry(r["value"], r.get("expires_at"))
                elif r["op"] == "delete":
                    self.memtable[r["key"]] = None

    def set(self, key, value, ttl_seconds=None):
        expires_at = None if not ttl_seconds else time.time() + ttl_seconds
        self._append_wal({"op":"set","key":key,"value":value,"expires_at":expires_at})
        self.memtable[key] = Entry(value, expires_at)
        if len(self.memtable) >= self.flush_threshold:
            self.flush()

    def get(self, key):
        if key in self.memtable:
            e = self.memtable[key]
            if e is None:
                return None
            if e.expires_at and e.expires_at <= time.time():
                self.memtable[key] = None
                return None
            return e.value
        if os.path.exists(self.sst_path):
            with open(self.sst_path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        r = json.loads(line)
                        if r["key"] == key:
                            if r.get("expires_at") and r["expires_at"] <= time.time():
                                return None
                            return r["value"]
        return None

    def delete(self, key):
        self._append_wal({"op":"delete","key":key})
        self.memtable[key] = None

    def flush(self):
        current = {}
        if os.path.exists(self.sst_path):
            with open(self.sst_path, encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        r = json.loads(line)
                        current[r["key"]] = r
        for key, e in self.memtable.items():
            if e is None:
                current.pop(key, None)
            else:
                current[key] = {"key":key,"value":e.value,"expires_at":e.expires_at}
        tmp = self.sst_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for key in sorted(current):
                f.write(json.dumps(current[key]) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.sst_path)
        self.memtable.clear()
