#!/bin/bash
# End-to-end verification. Prints PASS/FAIL per check, exits non-zero on any FAIL.
# Requires: docker compose, curl, python3.
set -uo pipefail
cd "$(dirname "$0")/.."

# shellcheck disable=SC1091
set -a; source .env; set +a

COMPOSE="docker compose"
FAILURES=0
DUMP_FILE="$(mktemp)"
trap 'rm -f "${DUMP_FILE}"' EXIT

pass() { echo "PASS  $1"; }
fail() { echo "FAIL  $1"; FAILURES=$((FAILURES + 1)); }

psql_source() { ${COMPOSE} exec -T postgres-source psql -U "${SOURCE_DB_USER}" -d "${SOURCE_DB_NAME}" -X -At -c "$1"; }
psql_sink()   { ${COMPOSE} exec -T postgres-sink   psql -U "${SINK_DB_USER}"   -d "${SINK_DB_NAME}"   -X -At -c "$1"; }

# --- 1. connector is RUNNING -------------------------------------------------
status_json=$(curl -sf http://localhost:8083/connectors/payments/status || echo '{}')
if python3 - "$status_json" <<'PY'
import json, sys
s = json.loads(sys.argv[1])
assert s.get("connector", {}).get("state") == "RUNNING"
assert s.get("tasks"), "no tasks"
assert all(t["state"] == "RUNNING" for t in s["tasks"])
PY
then pass "1. connector 'payments' and all tasks are RUNNING"
else fail "1. connector 'payments' is not RUNNING: ${status_json}"
fi

# --- 2. source table is partitioned with >= 3 partitions ---------------------
relkind=$(psql_source "SELECT relkind FROM pg_class WHERE relname = 'payments' AND relnamespace = 'public'::regnamespace;")
leaves=$(psql_source "SELECT count(*) FROM pg_partition_tree('payments'::regclass) WHERE isleaf;")
if [[ "${relkind}" == "p" && "${leaves:-0}" -ge 3 ]]
then pass "2. payments is partitioned (relkind=p) with ${leaves} leaf partitions"
else fail "2. payments partitioning wrong (relkind=${relkind}, leaf partitions=${leaves})"
fi

# --- 3. sink row count catches up with the source ----------------------------
# DISTINCT payment_id: the Kafka key (and the sink PK) is payment_id, so that is
# the identity the pipeline guarantees — one sink row per payment.
src_count=$(psql_source "SELECT count(DISTINCT payment_id) FROM payments WHERE tenant_id = '${TARGET_TENANT}';")
sink_count=0
for _ in $(seq 1 45); do
  sink_count=$(psql_sink "SELECT count(*) FROM payments;")
  [[ "${sink_count}" -ge "${src_count}" ]] && break
  sleep 2
done
if [[ "${sink_count}" -ge "${src_count}" ]]
then pass "3. sink caught up: source(${TARGET_TENANT})=${src_count}, sink=${sink_count}"
else fail "3. sink did not catch up: source(${TARGET_TENANT})=${src_count}, sink=${sink_count}"
fi

# --- 4. sink contains only the target tenant ---------------------------------
foreign=$(psql_sink "SELECT count(*) FROM payments WHERE tenant_id <> '${TARGET_TENANT}';")
if [[ "${foreign}" == "0" ]]
then pass "4. sink has no rows with tenant_id <> '${TARGET_TENANT}'"
else fail "4. sink contains ${foreign} rows of other tenants"
fi

# --- dump the whole topic once (used by checks 5 and 6) -----------------------
# Pick 3 random finalized payments first: they are never updated again, so
# source / topic / sink stay comparable while the generator keeps running.
sample=$(psql_source "SELECT payment_id || '|' || status || '|' || extract(epoch FROM updated_at)
                      FROM payments
                      WHERE tenant_id = '${TARGET_TENANT}'
                        AND finalized_at IS NOT NULL
                        AND updated_at < now() - interval '15 seconds'
                      ORDER BY random() LIMIT 3;")
sleep 2
if ! ${COMPOSE} run --rm --no-deps topicdump > "${DUMP_FILE}" 2>/dev/null; then
  fail "5. could not dump topic payments.state"
  fail "6. could not dump topic payments.state"
  echo; echo "${FAILURES} check(s) failed"; exit 1
fi

# --- 5. status/updated_at match across source, topic (latest) and sink -------
sink_rows=""
while IFS= read -r line; do
  pid="${line%%|*}"
  sink_rows+=$(psql_sink "SELECT payment_id || '|' || status || '|' || extract(epoch FROM updated_at)
                          FROM payments WHERE payment_id = '${pid}';" < /dev/null)$'\n'
done <<< "${sample}"

if python3 - "${DUMP_FILE}" "${sample}" "${sink_rows}" <<'PY'
import json, sys
from datetime import datetime

dump_file, sample, sink_rows = sys.argv[1], sys.argv[2], sys.argv[3]

def parse(rows):
    out = {}
    for line in rows.strip().splitlines():
        pid, status, epoch = line.split("|")
        out[pid] = (status, round(float(epoch), 6))
    return out

source, sink = parse(sample), parse(sink_rows)
assert len(source) == 3, f"expected 3 sampled payments, got {len(source)}"

latest = {}
with open(dump_file) as f:
    for line in f:
        rec = json.loads(line)
        v = rec.get("value")
        if v:
            ts = datetime.fromisoformat(v["updated_at"].replace("Z", "+00:00")).timestamp()
            latest[v["payment_id"]] = (v["status"], round(ts, 6))

for pid, (status, ts) in source.items():
    for name, store in (("topic", latest), ("sink", sink)):
        got = store.get(pid)
        assert got, f"{pid} missing in {name}"
        assert got[0] == status, f"{pid} status mismatch in {name}: {got[0]} != {status}"
        assert abs(got[1] - ts) < 0.001, f"{pid} updated_at mismatch in {name}: {got[1]} != {ts}"
    print(f"      {pid}: status={status} consistent across source/topic/sink", file=sys.stderr)
PY
then pass "5. 3 random payments consistent (status, updated_at) across source/topic/sink"
else fail "5. sampled payments inconsistent across source/topic/sink"
fi

# --- 6. every record in the topic belongs to the target tenant ----------------
if python3 - "${DUMP_FILE}" "${TARGET_TENANT}" <<'PY'
import json, sys
dump_file, target = sys.argv[1], sys.argv[2]
total = 0
with open(dump_file) as f:
    for line in f:
        rec = json.loads(line)
        v = rec.get("value")
        if v is None:
            continue
        total += 1
        assert v["tenant_id"] == target, f"foreign tenant {v['tenant_id']} at partition={rec['partition']} offset={rec['offset']}"
assert total > 0, "topic is empty"
print(f"      {total} records checked, all tenant_id={target}", file=sys.stderr)
PY
then pass "6. all records in payments.state have tenant_id = '${TARGET_TENANT}'"
else fail "6. payments.state contains records of other tenants"
fi

# --- 7. sensitive column card_token never leaves the source -------------------
# Populated in the source, dropped by the connector (column.exclude.list), so it
# must appear in no topic record and have no column in the sink at all.
src_missing=$(psql_source "SELECT count(*) FROM payments WHERE card_token IS NULL;")
sink_col=$(psql_sink "SELECT count(*) FROM information_schema.columns
                      WHERE table_name = 'payments' AND column_name = 'card_token';")
if python3 - "${DUMP_FILE}" <<'PY'
import json, sys
leaked = 0
with open(sys.argv[1]) as f:
    for line in f:
        v = json.loads(line).get("value")
        if v and "card_token" in v:
            leaked += 1
assert leaked == 0, f"{leaked} topic records contain card_token"
PY
then topic_ok=1; else topic_ok=0; fi
if [[ "${src_missing}" == "0" && "${sink_col}" == "0" && "${topic_ok}" == "1" ]]
then pass "7. card_token present in source, absent from topic and sink"
else fail "7. card_token leak check (source NULLs=${src_missing}, sink column=${sink_col}, topic clean=${topic_ok})"
fi

echo
if [[ ${FAILURES} -eq 0 ]]
then echo "All checks PASSED"
else echo "${FAILURES} check(s) FAILED"; exit 1
fi
