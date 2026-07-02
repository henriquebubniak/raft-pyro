FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# generate the gRPC stubs during the build: servers implement BOTH
# contracts (internal raft + application API)
COPY proto ./proto
RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. \
    proto/server.proto proto/client.proto

COPY main.py viz.py ./

CMD ["python", "main.py"]
