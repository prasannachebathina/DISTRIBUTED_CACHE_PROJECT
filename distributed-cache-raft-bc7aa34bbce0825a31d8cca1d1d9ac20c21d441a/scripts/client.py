import sys
import grpc
from proto import cache_pb2, cache_pb2_grpc

port = 50051

if "--port" in sys.argv:
    i = sys.argv.index("--port")
    port = int(sys.argv[i + 1])
    del sys.argv[i:i + 2]

channel = grpc.insecure_channel(f"localhost:{port}")
stub = cache_pb2_grpc.CacheServiceStub(channel)

op = sys.argv[1]

if op == "set":
    print(stub.Set(
        cache_pb2.SetRequest(
            key=sys.argv[2],
            value=sys.argv[3],
            request_id="cli-1"
        )
    ))

elif op == "get":
    print(stub.Get(
        cache_pb2.GetRequest(key=sys.argv[2])
    ))

elif op == "delete":
    print(stub.Delete(
        cache_pb2.DeleteRequest(
            key=sys.argv[2],
            request_id="cli-1"
        )
    ))

else:
    raise SystemExit(
        "Usage: set KEY VALUE | get KEY | delete KEY [--port PORT]"
    )
