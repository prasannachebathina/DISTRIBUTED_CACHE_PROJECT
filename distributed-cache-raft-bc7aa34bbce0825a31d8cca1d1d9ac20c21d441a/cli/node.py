import argparse
from dcache.node import serve

p = argparse.ArgumentParser()
p.add_argument("--node-id", required=True)
p.add_argument("--port", type=int, required=True)
p.add_argument("--metrics-port", type=int, required=True)
p.add_argument("--peers", default="")
a = p.parse_args()
serve(a.node_id, a.port, [x for x in a.peers.split(",") if x], a.metrics_port)
