# build context is raft-grpc/ (see docker-compose.yml), so the shared
# proto/ is available and the Go stubs are generated during the build —
# same approach as the Python image
FROM golang:1.25 AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest \
    && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

WORKDIR /src
COPY client-go/go.mod ./
COPY client-go/main.go ./
# the client is built ONLY against the application contract: it never
# even generates stubs for the internal raft protocol (server.proto)
COPY proto/client.proto ./proto/client.proto

RUN protoc -I proto \
    --go_out=. --go_opt=module=raftclient \
    --go-grpc_out=. --go-grpc_opt=module=raftclient \
    proto/client.proto

RUN go mod tidy && CGO_ENABLED=0 go build -o /client .

FROM alpine:3.20
COPY --from=build /client /usr/local/bin/client
ENTRYPOINT ["client"]
