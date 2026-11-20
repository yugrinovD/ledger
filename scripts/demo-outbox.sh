#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

ensure_core
ensure_seed
API_KEY="$(new_api_key outbox-demo)"
mapfile -t ACCOUNTS < <(account_ids)
SOURCE="${ACCOUNTS[0]}"
DESTINATION="${ACCOUNTS[1]}"

echo "==> stopping Redpanda; the API must keep accepting transfers"
"${COMPOSE[@]}" stop redpanda >/dev/null
post_transfer "outbox-$(date +%s)" "$SOURCE" "$DESTINATION" "1.00" | jq '{id, status, amount, currency}'
sleep 1
echo
echo "==> outbox backlog while the broker is down"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl outbox status 2>/dev/null | jq .
echo
echo "==> starting Redpanda again"
"${COMPOSE[@]}" up -d --wait redpanda >/dev/null
sleep 5
echo
echo "==> outbox backlog after the relay reconnects"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl outbox status 2>/dev/null | jq .
echo
echo "The transfer was never lost: it was committed in PostgreSQL and published later."
