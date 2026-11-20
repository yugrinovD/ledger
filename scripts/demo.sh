#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

ensure_core
ensure_seed
API_KEY="$(new_api_key demo)"
mapfile -t ACCOUNTS < <(account_ids)
SOURCE="${ACCOUNTS[0]}"
DESTINATION="${ACCOUNTS[1]}"
KEY="demo-$(date +%s)"

echo
echo "==> transfer ${KEY}"
post_transfer "$KEY" "$SOURCE" "$DESTINATION" "10.00" | jq .
echo
echo "==> exact duplicate request with the same Idempotency-Key"
curl -sS -D /tmp/ledger-demo-headers -o /tmp/ledger-demo-body \
  -X POST "${API_URL}/v1/transfers" \
  -H "Authorization: Bearer ${API_KEY}" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: ${KEY}" \
  -d "{\"source_account_id\":\"${SOURCE}\",\"destination_account_id\":\"${DESTINATION}\",\"amount\":\"10.00\",\"currency\":\"USD\",\"description\":\"demo\"}"
grep -i -E '^(HTTP|Idempotent-Replayed)' /tmp/ledger-demo-headers || true
jq . /tmp/ledger-demo-body
echo
echo "==> destination balance"
curl -fsS "${API_URL}/v1/accounts/${DESTINATION}/balance" \
  -H "Authorization: Bearer ${API_KEY}" | jq .
echo
echo "==> reconciliation"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl reconcile 2>/dev/null | jq '{status: .run.status, checked_accounts: .run.checked_accounts, discrepancy_count: .run.discrepancy_count}'
echo
echo "Demo complete. If the observability profile is running: Grafana http://localhost:3000, Jaeger http://localhost:16686, Kafka UI http://localhost:8082."
