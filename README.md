# pstream — single-tenant payment state streaming (CDC POC)

Proof of concept for the pattern **"full payment state in Kafka, key = `payment_id`,
downstream consumer upserts into its own database"**, where the source table holds
many tenants but the Kafka topic carries exactly one.

```
partitioned Postgres ──▶ Debezium (pgoutput) ──▶ Kafka (Avro, compacted) ──▶ Go sink ──▶ Postgres
   3 tenants              publication filters        payments.state            upsert guarded
   monthly partitions     to one tenant              key = payment_id          by updated_at
```

## The pattern

This is event-carried state transfer: every change ships the **full row after the
change** (no Debezium `before/after` envelope — `ExtractNewRecordState` unwraps it),
so any consumer can rebuild its own queryable copy with a plain upsert and no joins.
The topic is **compacted** with key `payment_id`, so it converges towards exactly one
record per payment — a snapshot of current state that any new consumer can bootstrap
from, while recent history (several records per payment with different `status`)
remains visible until compaction runs.

The Postgres primary key is composite — `(payment_id, created_at)` — because
Postgres requires the partition key inside the PK of a partitioned table. The Kafka
key is still **`payment_id` only** (`message.key.columns`): downstream identity is
the payment, and one payment must always land in the same topic partition for
ordering, regardless of which table partition it lives in.

The tenant filter lives **in the publication** (`CREATE PUBLICATION … WHERE
tenant_id = …`), not in an SMT: rows of other tenants never leave the database
process at all. Two things are required for that to be correct:

- **`REPLICA IDENTITY FULL`** on the parent *and every partition* — the row filter
  references `tenant_id`, which is not part of the PK, so Postgres refuses to
  publish UPDATE/DELETE without it. Replica identity is a per-partition property;
  it is set in the same place partitions are created (`postgres/init/01_schema.sql.tmpl`).
- **The snapshot override** (`snapshot.select.statement.overrides`) — Debezium's
  initial snapshot reads the table directly and *bypasses the publication*, so
  without an explicit `WHERE tenant_id = …` the other tenants would leak into the
  topic on the first load.

The sink upsert is guarded by `WHERE payments.updated_at < EXCLUDED.updated_at`,
which makes it idempotent: replays (e.g. after a re-snapshot) and reordered
deliveries can never overwrite newer state with older state.

## Active variant

The **publication row filter works** on this PostgreSQL 18 / Debezium 3.7
combination (with `publish_via_partition_root = true` streaming arrives from the
parent `public.payments`, and the snapshot of the parent covers all partitions with
`table.include.list = public.payments` — no per-partition listing needed). The
fallback described in the plan (unfiltered publication + Debezium scripting
`Filter` SMT) was therefore **not** needed and is not wired in.

## Quick start

Requires Docker with Compose v2. For `make verify` also `curl` and `python3`.

```bash
make up       # renders templates, builds, starts everything, waits for health
make verify   # 6 PASS/FAIL checks (connector, partitioning, counts, tenant isolation)
make logs     # follow all logs
make down     # stop (keep volumes)
make clean    # stop and delete volumes
```

`make up` finishes with all services healthy and the connector registered —
no manual steps. Configuration lives in `.env` (image versions, `TARGET_TENANT`,
generator settings). All credentials there are throwaway test values.

| UI / API        | URL                    |
|-----------------|------------------------|
| AKHQ            | http://localhost:8080  |
| pgAdmin         | http://localhost:5050  |
| Kafka Connect   | http://localhost:8083  |
| Schema Registry | http://localhost:8081  |

pgAdmin opens without a login (desktop mode, POC only) with both databases
pre-connected: **source (payments)** — the partitioned multi-tenant table — and
**sink** — the single-tenant copy the consumer maintains.

## What to look at in AKHQ

- **Topics → `payments.state`**: live data, decoded Avro. The key is
  `{"payment_id": …}` only; the value is the full 15-field row. Search for one
  `payment_id` to see its lifecycle (`created → processing → success/failed/refunded`).
- **Topics → `payments.state` → Configs**: `cleanup.policy=compact`,
  `min.compaction.lag.ms=60000`, `retention.ms=-1`.
- **Schema Registry**: subjects `payments.state-key` / `payments.state-value`.
- **Connect**: connector `payments`, state RUNNING.

## Observing compaction

Compaction only rewrites *closed* segments, and the active segment rolls slowly
with default settings. To see it quickly:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name payments.state \
  --add-config segment.ms=60000,min.cleanable.dirty.ratio=0.01
```

Wait a few minutes, then compare the record count (`docker compose run --rm topicdump | wc -l`)
with the number of distinct payments — old statuses of finalized payments disappear,
the latest record per `payment_id` survives. `kafka-log-dirs.sh --describe --topic-list
payments.state` shows segment sizes shrinking.

## Re-snapshot (rebuild the topic from the table)

```bash
curl -X PUT  localhost:8083/connectors/payments/stop
curl -X DELETE localhost:8083/connectors/payments/offsets
docker compose exec postgres-source psql -U postgres -d payments \
  -c "SELECT pg_drop_replication_slot('dbz_payments');"
curl -X PUT  localhost:8083/connectors/payments/resume
```

The connector re-runs the (tenant-filtered) initial snapshot and then streams on.
Duplicates are harmless: the topic is compacted and the sink upsert is guarded by
`updated_at`.

## Known POC limitations

- Single-node Kafka, replication factor 1, no TLS/SASL — not a production setup.
- The sink commits offsets per record (simple, slow); batching would be the first
  production optimization. Delivery is at-least-once; the upsert guard provides
  effective idempotency.
- Deletes are dropped (`delete.tombstone.handling.mode=drop`); the sink never
  removes rows. For GDPR-style erasure you would emit tombstones instead and
  handle them in the sink.
- New monthly partitions beyond ±1 month are not auto-created; the `DEFAULT`
  partition catches stragglers. Production would use `pg_partman` or a cron job
  (and must set `REPLICA IDENTITY FULL` on each new partition).
- One connector per tenant topic. Many tenants ⇒ many publications/slots; that
  trade-off is the point of this POC, but slot count is a real Postgres limit.
- `numeric` values travel as strings (`decimal.handling.mode=string`) to avoid
  Avro `decimal` logical-type friction in non-JVM consumers; `timestamptz`
  travels as ISO-8601 strings (Debezium `ZonedTimestamp`). Postgres parses both
  back losslessly on the sink side, so no column types were changed.

## Repository layout

```
docker-compose.yml        # all services, healthchecks, dependency ordering
.env                      # pinned image versions, TARGET_TENANT, generator settings
Makefile                  # up, down, logs, connector, verify, clean
postgres/
  postgresql.conf         # wal_level=logical etc.
  init/01_schema.sql.tmpl # partitioned table, replica identity, filtered publication
  sink-init/01_schema.sql # downstream table, PK = payment_id
connect/
  Dockerfile              # debezium/connect + Confluent Avro converter
  payments-connector.json.tmpl
akhq/application.yml
pgadmin/                  # servers.json + pgpass templates (both DBs pre-connected)
generator/                # Go: multi-tenant inserts + lifecycle updates (pgx only)
sink/                     # Go: Kafka consumer -> upsert (franz-go, hamba/avro, pgx)
scripts/                  # render.sh, register-connector.sh, verify.sh
```

`TARGET_TENANT` appears in exactly three places: the publication row filter, the
connector snapshot override (both rendered from `.tmpl` by `scripts/render.sh`),
and `make verify`.

## License

MIT — see [LICENSE](LICENSE).
