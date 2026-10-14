#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python -m grpc_tools.protoc \
  -I src \
  --python_out=src \
  --grpc_python_out=src \
  --mypy_out=src \
  --mypy_grpc_out=src \
  dfs/proto/raft.proto
