# Distributed Cache with Consistent Hashing — Python/Linux

Academic trimester project using Python on Ubuntu/Linux.

Core components:
- Consistent hashing + virtual nodes
- GET / SET / DELETE
- TTL + LRU
- WAL + memtable + SSTable-style persistence
- gRPC communication
- Raft state/log core
- Prometheus metrics
- Docker/Kubernetes/Helm skeleton
- Fault-injection and benchmark scripts

Start with:
```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip git curl build-essential protobuf-compiler
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
bash scripts/generate_proto.sh
python -m pytest -q
```
