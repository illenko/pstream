#!/bin/bash
# Idempotently registers (creates or updates) the payments connector.
# Runs inside the compose network; also reachable via `make connector`.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
CONFIG_FILE="${CONFIG_FILE:-/config/payments-connector.json}"

echo "waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do
  sleep 2
done

echo "registering connector 'payments' ..."
http_code=$(curl -s -o /tmp/register-response -w '%{http_code}' \
  -X PUT -H "Content-Type: application/json" \
  --data @"${CONFIG_FILE}" \
  "${CONNECT_URL}/connectors/payments/config")

cat /tmp/register-response
echo
if [[ "${http_code}" != "200" && "${http_code}" != "201" ]]; then
  echo "connector registration failed with HTTP ${http_code}" >&2
  exit 1
fi
echo "connector registered (HTTP ${http_code})"
