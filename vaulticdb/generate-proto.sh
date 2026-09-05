#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
proto_dir="$repo_root/vaulticdb/proto"
go_out="$repo_root/internal/index/proto"

require_version() {
    tool=$1
    expected=$2
    install_hint=$3

    command -v "$tool" >/dev/null 2>&1 || {
        echo "vaulticdb: $tool is required" >&2
        echo "$install_hint" >&2
        exit 1
    }

    actual=$("$tool" --version 2>&1)
    if [ "$actual" != "$expected" ]; then
        echo "vaulticdb: expected $expected, found $actual" >&2
        echo "$install_hint" >&2
        exit 1
    fi
}

require_version protoc "libprotoc 36.0" \
    "install protoc 36.0 from https://github.com/protocolbuffers/protobuf/releases/tag/v36.0"
require_version protoc-gen-go "protoc-gen-go v1.36.10" \
    "install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10"
require_version protoc-gen-go-grpc "protoc-gen-go-grpc 1.5.1" \
    "install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1"

mkdir -p "$go_out"
protoc \
    --proto_path="$proto_dir" \
    --go_out="$go_out" \
    --go_opt=paths=source_relative \
    --go-grpc_out="$go_out" \
    --go-grpc_opt=paths=source_relative \
    "$proto_dir/vaulticdb/v1/daemon.proto"
