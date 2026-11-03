#!/usr/bin/env bash
# End-to-end checks against a running stack (`make up`). Needs curl and jq.
#
#   ./scripts/smoke.sh                 # against http://localhost:8080
#   BASE_URL=http://host:8080 ./scripts/smoke.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
pass=0

ok() { pass=$((pass + 1)); printf '  ok  %s\n' "$1"; }
fail() { printf '  FAIL %s\n' "$1" >&2; exit 1; }

# request METHOD PATH [BODY] [extra curl args...] -> sets $status and $body
request() {
  local method=$1 path=$2 data=${3:-}
  shift 3 || shift $#
  local out
  out=$(curl -s -w '\n%{http_code}' -X "$method" "$BASE_URL$path" \
    -H 'Content-Type: application/json' ${data:+-d "$data"} "$@")
  status=$(tail -n1 <<<"$out")
  body=$(sed '$d' <<<"$out")
}

stock() {
  request GET /products
  jq -r --arg id "$1" '.products[] | select(.id == $id) | .stock' <<<"$body"
}

stock_is() { [[ $(stock "$1") == "$2" ]]; }

# wait_for DESCRIPTION COMMAND... : retry until the command succeeds (async effects)
wait_for() {
  local what=$1; shift
  for _ in $(seq 1 120); do
    if "$@"; then ok "$what"; return; fi
    sleep 0.25
  done
  fail "$what"
}

create_order() { # product quantity [idempotency key]
  request POST /orders "{\"customer_email\":\"smoke@example.com\",\"items\":[{\"product_id\":\"$1\",\"quantity\":$2}]}" \
    ${3:+-H "Idempotency-Key: $3"}
}

echo "smoke tests against $BASE_URL"

gateway_up() { request GET /health && [[ $status == 200 ]]; }
wait_for "gateway is healthy" gateway_up

# The stack is up once order-service answers through the gateway.
products_up() { request GET /products && [[ $status == 200 ]]; }
wait_for "product catalog is reachable" products_up

# --- happy path --------------------------------------------------------------
before=$(stock prod-coffee-001)
create_order prod-coffee-001 2
[[ $status == 201 ]] || fail "create order: $status $body"
order_id=$(jq -r .order.id <<<"$body")
[[ $(jq -r .order.status <<<"$body") == stock_reserved ]] && ok "order reserves stock" || fail "status: $body"
[[ $(stock prod-coffee-001) == $((before - 2)) ]] && ok "stock decremented" || fail "stock not decremented"

request POST "/orders/$order_id/pay" '' -H "Idempotency-Key: pay-$order_id"
[[ $status == 200 && $(jq -r .order.status <<<"$body") == paid ]] && ok "order is paid" || fail "pay: $status $body"

request POST "/orders/$order_id/pay" '' -H "Idempotency-Key: pay-$order_id-retry"
[[ $status == 200 && $(jq -r .order.status <<<"$body") == paid ]] && ok "paying again is a no-op" || fail "repay: $status $body"

# --- concurrent pay ----------------------------------------------------------
create_order prod-mug-003 1
order_id=$(jq -r .order.id <<<"$body")
for i in $(seq 1 10); do
  curl -s -o /dev/null -X POST "$BASE_URL/orders/$order_id/pay" -H "Idempotency-Key: race-$i" &
done
wait
request GET "/orders/$order_id"
[[ $(jq -r .order.status <<<"$body") == paid ]] && ok "10 concurrent pays end in one paid order" || fail "race: $body"

# --- cancel releases stock asynchronously -------------------------------------
before=$(stock prod-tea-002)
create_order prod-tea-002 3
order_id=$(jq -r .order.id <<<"$body")
request POST "/orders/$order_id/cancel" ''
[[ $status == 200 && $(jq -r .order.status <<<"$body") == cancelled ]] && ok "order cancelled" || fail "cancel: $status $body"
wait_for "cancelled order's stock is released via order.cancelled" stock_is prod-tea-002 "$before"

request POST "/orders/$order_id/pay" '' -H "Idempotency-Key: late"
[[ $status == 409 ]] && ok "cancelled order cannot be paid" || fail "pay cancelled: $status"

# --- declined payment ---------------------------------------------------------
before=$(stock prod-keyboard-007)
create_order prod-keyboard-007 17   # 17 x $59.99 is above the default decline limit
order_id=$(jq -r .order.id <<<"$body")
request POST "/orders/$order_id/pay" '' -H "Idempotency-Key: big-$order_id"
[[ $(jq -r .order.status <<<"$body") == failed ]] && ok "large payment is declined" || fail "decline: $body"
wait_for "declined order's stock is released via order.failed" stock_is prod-keyboard-007 "$before"

# --- validation and errors -----------------------------------------------------
create_order prod-lamp-009 100000
[[ $status == 409 ]] && ok "insufficient stock is rejected" || fail "oversell: $status $body"

request POST /orders '{"customer_email":"nope","items":[]}'
[[ $status == 400 ]] && ok "invalid input is a 400" || fail "validation: $status"

request GET /orders/does-not-exist
[[ $status == 404 ]] && ok "unknown order is a 404" || fail "not found: $status"

# --- idempotent create ---------------------------------------------------------
key="create-$(date +%s%N)"
create_order prod-pen-006 1 "$key"; first=$(jq -r .order.id <<<"$body")
create_order prod-pen-006 1 "$key"; second=$(jq -r .order.id <<<"$body")
[[ $first == "$second" ]] && ok "create with the same Idempotency-Key returns the same order" || fail "idempotent create"

echo "$pass checks passed"
