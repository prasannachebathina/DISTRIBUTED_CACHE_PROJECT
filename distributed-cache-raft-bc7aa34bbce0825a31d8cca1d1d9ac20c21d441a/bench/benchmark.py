import time, statistics, grpc
from proto import cache_pb2, cache_pb2_grpc

stub = cache_pb2_grpc.CacheServiceStub(
    grpc.insecure_channel("localhost:50051"))

times = []
for i in range(100):
    t = time.perf_counter()
    stub.Set(cache_pb2.SetRequest(key=f"k{i}", value=f"v{i}"))
    times.append((time.perf_counter()-t)*1000)

times.sort()
print("requests =", len(times))
print("average_ms =", statistics.mean(times))
print("p50_ms =", times[len(times)//2])
print("p95_ms =", times[int(.95*len(times))-1])
print("p99_ms =", times[int(.99*len(times))-1])
