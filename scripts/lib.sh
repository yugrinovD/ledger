#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
COMPOSE=(docker compose)
API_URL="${LEDGER_API_URL:-http://localhost:${LEDGER_API_PORT:-8080}}"
ADMIN_URL="${LEDGER_ADMIN_URL:-http://localhost:${LEDGER_API_ADMIN_PORT:-9090}}"

wait_ready() {
  for _ in $(seq 1 60); do
    if curl -fsS "${ADMIN_URL}/readyz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "ledger-api did not become ready" >&2
  return 1
}

ensure_core() {
  "${COMPOSE[@]}" up -d --build postgres redis redpanda migrate api worker
  wait_ready
}

ensure_seed() {
  if [[ ! -f load/accounts.json ]]; then
    "${COMPOSE[@]}" --profile tools run --rm ledgerctl seed --customers 5 --output /out/accounts.json >/dev/null
  fi
}

new_api_key() {
  "${COMPOSE[@]}" --profile tools run --rm ledgerctl apikey-create \
    --owner demo --name "${1:-demo}" --scopes '*' 2>/dev/null | jq -r .key
}

account_ids() {
  jq -r '.accounts[0].id, .accounts[1].id' load/accounts.json
}

post_transfer() {
  local key="$1" source="$2" destination="$3" amount="$4"
  curl -fsS -X POST "${API_URL}/v1/transfers" \
    -H "Authorization: Bearer ${API_KEY}" \
    -H 'Content-Type: application/json' \
    -H "Idempotency-Key: ${key}" \
    -d "{\"source_account_id\":\"${source}\",\"destination_account_id\":\"${destination}\",\"amount\":\"${amount}\",\"currency\":\"USD\",\"description\":\"demo\"}"
}
