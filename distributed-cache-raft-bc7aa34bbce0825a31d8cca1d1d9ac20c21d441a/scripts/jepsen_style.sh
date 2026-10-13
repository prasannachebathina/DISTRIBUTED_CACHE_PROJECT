#!/usr/bin/env bash
set -e
echo "Jepsen-style smoke test"
for i in $(seq 1 20); do
  python scripts/client.py set key$i value$i >/dev/null
done
echo "Stop node2 to simulate a fault."
[ -f logs/node2.pid ] && kill "$(cat logs/node2.pid)" || true
sleep 2
echo "Read committed keys and document observed behavior."
for i in $(seq 1 20); do
  python scripts/client.py get key$i
done
