#!/usr/bin/env bash
set -e
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/cache.proto
echo "gRPC Python files generated."
