#!/usr/bin/env bash
set -e
mkdir -p logs
source .venv/bin/activate
python -m cli.node --node-id=node1 --port=50051 --metrics-port=8001 --peers=node2,node3 > logs/node1.log 2>&1 &
echo $! > logs/node1.pid
python -m cli.node --node-id=node2 --port=50052 --metrics-port=8002 --peers=node1,node3 > logs/node2.log 2>&1 &
echo $! > logs/node2.pid
python -m cli.node --node-id=node3 --port=50053 --metrics-port=8003 --peers=node1,node2 > logs/node3.log 2>&1 &
echo $! > logs/node3.pid
echo "Started node1/node2/node3."
