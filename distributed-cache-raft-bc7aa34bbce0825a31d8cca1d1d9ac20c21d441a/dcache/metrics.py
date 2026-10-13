from prometheus_client import Counter, Histogram

REQUESTS = Counter("dcache_requests_total", "Cache requests", ["operation"])
LATENCY = Histogram("dcache_request_latency_seconds", "Request latency", ["operation"])
HITS = Counter("dcache_hits_total", "Cache hits")
MISSES = Counter("dcache_misses_total", "Cache misses")
