#!/bin/bash
# Renders *.tmpl files, substituting __TARGET_TENANT__ from .env.
# TARGET_TENANT is intentionally used in exactly three places:
# the publication row filter, the connector snapshot override, and `make verify`.
set -euo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
set -a; source .env; set +a

if [[ -z "${TARGET_TENANT:-}" ]]; then
  echo "TARGET_TENANT is not set in .env" >&2
  exit 1
fi

render() {
  local tmpl="$1" out="${1%.tmpl}"
  sed -e "s/__TARGET_TENANT__/${TARGET_TENANT}/g" \
      -e "s/__DEBEZIUM_DB_USER__/${DEBEZIUM_DB_USER}/g" \
      -e "s/__DEBEZIUM_DB_PASSWORD__/${DEBEZIUM_DB_PASSWORD}/g" \
      "${tmpl}" > "${out}"
  echo "rendered ${out} (TARGET_TENANT=${TARGET_TENANT})"
}

render postgres/init/01_schema.sql.tmpl
render connect/payments-connector.json.tmpl
