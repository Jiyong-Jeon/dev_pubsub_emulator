#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_ROOT/protos"
OUTPUT_DIR="$PROJECT_ROOT/src/dev_pubsub/generated"

GOOGLEAPIS_VERSION="master"
GOOGLEAPIS_BASE="https://raw.githubusercontent.com/googleapis/googleapis/${GOOGLEAPIS_VERSION}"

echo "==> Downloading proto files..."
mkdir -p "$PROTO_DIR/google/pubsub/v1"
mkdir -p "$PROTO_DIR/google/api"
mkdir -p "$PROTO_DIR/google/protobuf"

# Pub/Sub proto
curl -sL "${GOOGLEAPIS_BASE}/google/pubsub/v1/pubsub.proto" -o "$PROTO_DIR/google/pubsub/v1/pubsub.proto"
curl -sL "${GOOGLEAPIS_BASE}/google/pubsub/v1/schema.proto" -o "$PROTO_DIR/google/pubsub/v1/schema.proto"

# API protos (dependencies)
for f in annotations.proto http.proto client.proto field_behavior.proto field_info.proto resource.proto launch_stage.proto; do
    curl -sL "${GOOGLEAPIS_BASE}/google/api/${f}" -o "$PROTO_DIR/google/api/${f}"
done

echo "==> Compiling proto files..."
mkdir -p "$OUTPUT_DIR/google/pubsub/v1"

python -m grpc_tools.protoc \
    --proto_path="$PROTO_DIR" \
    --python_out="$OUTPUT_DIR" \
    --grpc_python_out="$OUTPUT_DIR" \
    "google/pubsub/v1/schema.proto" \
    "google/pubsub/v1/pubsub.proto"

echo "==> Fixing import paths..."
# Fix generated imports to use package-relative paths
cd "$OUTPUT_DIR"
for f in google/pubsub/v1/*.py; do
    # Fix: from google.pubsub.v1 import -> from dev_pubsub.generated.google.pubsub.v1 import
    sed -i 's/^from google\.pubsub\.v1 import/from dev_pubsub.generated.google.pubsub.v1 import/g' "$f"
    # Fix: from google.api import -> from google.api import (keep, googleapis-common-protos provides these)
done

echo "==> Creating __init__.py files..."
for d in google google/pubsub google/pubsub/v1; do
    touch "$OUTPUT_DIR/$d/__init__.py"
done

echo "==> Done! Generated files:"
find "$OUTPUT_DIR/google" -name "*.py" -type f | sort
