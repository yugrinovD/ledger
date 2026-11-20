#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/lib.sh"

ensure_core
ensure_seed
mapfile -t ACCOUNTS < <(account_ids)
ACCOUNT="${ACCOUNTS[0]}"

echo "==> corrupting the balance projection by +500 minor units"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl debug corrupt-balance \
  --account "$ACCOUNT" --delta-minor 500 2>/dev/null | jq .
echo
echo "==> reconciliation without repair"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl reconcile 2>/dev/null \
  | jq '{status: .run.status, discrepancy_count: .run.discrepancy_count, discrepancies: [.discrepancies[] | {kind, account_id, expected, actual}]}'
echo
echo "==> reconciliation with repair"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl reconcile --repair 2>/dev/null \
  | jq '{status: .run.status, discrepancy_count: .run.discrepancy_count, repaired_accounts: .run.stats.repaired_accounts}'
echo
echo "==> clean reconciliation"
"${COMPOSE[@]}" --profile tools run --rm ledgerctl reconcile 2>/dev/null \
  | jq '{status: .run.status, discrepancy_count: .run.discrepancy_count}'
