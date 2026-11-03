#!/usr/bin/env sh
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$ROOT_DIR"

PATH="$HOME/go/bin:$PATH"
export PATH

mkdir -p gen

protoc \
  --go_out=. --go_opt=module=github.com/AlexW6385/distributed-order-processing-system \
  --go-grpc_out=. --go-grpc_opt=module=github.com/AlexW6385/distributed-order-processing-system \
  proto/product/v1/product.proto \
  proto/payment/v1/payment.proto \
  proto/order/v1/order.proto
