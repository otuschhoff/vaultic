#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
proto_dir="$repo_root/vaulticd/proto"
go_out="$repo_root/internal/index/proto"

command -v protoc >/dev/null 2>&1 || {
    echo "vaulticd: protoc is required to generate protobuf bindings" >&2
    exit 1
}
command -v protoc-gen-go >/dev/null 2>&1 || {
    echo "vaulticd: protoc-gen-go is required" >&2
    echo "install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest" >&2
    exit 1
}
command -v protoc-gen-go-grpc >/dev/null 2>&1 || {
    echo "vaulticd: protoc-gen-go-grpc is required" >&2
    echo "install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest" >&2
    exit 1
}

mkdir -p "$go_out"
protoc \
    --proto_path="$proto_dir" \
    --go_out="$go_out" \
    --go_opt=paths=source_relative \
    --go-grpc_out="$go_out" \
    --go-grpc_opt=paths=source_relative \
    "$proto_dir/vaulticd/v1/daemon.proto"
