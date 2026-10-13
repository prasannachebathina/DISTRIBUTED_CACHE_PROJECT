FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. proto/cache.proto
CMD ["python","-m","cli.node","--node-id=node1","--port=50051","--metrics-port=8001"]
