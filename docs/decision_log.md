# Decision Log | Started Mid way into project, will gradually update

Architectural decisions for the F1 data pipeline.
Each entry follows the ADR (Architecture Decision Record) format:
**Context → Decision → Consequences**.

---

## ADR-001 — BigQuery bronze layer partitioning strategy

**Date:** 2026-03-24
**Status:** Accepted

### Context

The bronze layer in BigQuery holds 12 tables sourced from the OpenF1 API via MinIO.
When migrating from ClickHouse (which used `PARTITION BY year`), we needed a BigQuery
partitioning strategy for tables that lack a direct event-time column.

Initial approach used `PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(2018, 2031, 1))`
on tables without a timestamp field (meetings, sessions, drivers, laps, stints).

Problems with `RANGE_BUCKET(year)`:
- Creates one partition per calendar year — very coarse; does not help query pruning
  for typical F1 queries that filter by session or meeting, not by year range
- As years accumulate, partitions grow unbounded in row count
- BigQuery's query optimizer gets little benefit since year is already low cardinality

### Decision

Apply the following per-table strategy:

| Table | Partitioning | Rationale |
|---|---|---|
| positions, intervals, car_data, locations, pits, race_control, team_radio | `PARTITION BY DATE(date)` | Non-null timestamp column available; DAY granularity aligns with race sessions |
| laps | `PARTITION BY DATE(_loaded_at)` | `date_start` is nullable (aborted laps have no start time); `_loaded_at` is always populated |
| meetings, sessions, drivers, stints | Clustering only — no partition | Tables are small (<10 GB/year); partition overhead outweighs benefit; clustering on `(meeting_key, session_key, driver_number)` prunes effectively for all analytics queries |

All tables cluster on `(meeting_key, session_key, driver_number)` where applicable,
mirroring ClickHouse ORDER BY keys.

### Consequences

- **Positive:** High-volume time-series tables (car_data, positions, locations) benefit
  from day-level partition pruning — a single race day query scans one partition instead
  of a full table scan.
- **Positive:** Small reference tables avoid unnecessary partition metadata overhead.
- **Positive:** `DATE(_loaded_at)` on laps is consistent and always non-null; the tradeoff
  is that filtering by event date on laps requires a full scan of the relevant load partitions
  rather than event-time pruning.
- **Negative:** laps queries filtered by `date_start` will not benefit from partition pruning;
  clustering on `(meeting_key, session_key, driver_number)` mitigates this for typical queries.
- **Future option:** If laps event-time pruning becomes important, add a non-nullable
  `session_date DATE` column injected at load time and repartition.

---

## ADR-002 — Bronze layer storage: ClickHouse → BigQuery

**Date:** 2026-03-24
**Status:** Accepted

### Context

The original bronze layer used ClickHouse with `ReplacingMergeTree` for deduplication,
running as a Docker container alongside Airflow. This caused persistent operational issues:

- **Memory pressure:** ClickHouse consumed 5 GB of the 9 GB Docker Desktop allocation,
  leaving insufficient headroom for Airflow workers. High-volume loaders (car_data, positions,
  locations at ~3.7 Hz) caused repeated OOM kills and required chunked inserts as a workaround.
- **Docker daemon instability:** ClickHouse's resource footprint contributed to Docker Desktop
  crashes that prevented Airflow containers from starting.
- **Limited scalability:** Vertical scaling on a local machine has a hard ceiling.

### Decision

Migrate the bronze (and subsequent silver/gold) layers to BigQuery (GCP managed service).
MinIO continues to store raw JSON as the immutable source-of-truth archive.

Pipeline becomes:
```
OpenF1 API → JSON in MinIO (raw archive, unchanged)
                  ↓
         BigQuery bronze (load jobs via google-cloud-bigquery)
                  ↓
         dbt silver → dbt gold  (dbt-bigquery adapter)
                  ↓
              Superset / BI layer (BigQuery connector)
```

### Consequences

- **Positive:** Eliminates ClickHouse memory contention; Docker Desktop only runs Airflow + MinIO.
- **Positive:** BigQuery scales storage and compute independently with no operational overhead.
- **Positive:** Partition pruning and clustering replace `ReplacingMergeTree` ORDER BY semantics.
- **Negative:** BigQuery free tier restricts streaming inserts; load jobs (batch) are used instead,
  introducing a small latency compared to streaming.
- **Negative:** `MERGE`-based deduplication in dbt replaces ClickHouse's native `ReplacingMergeTree`
  async dedup; dbt silver models are slightly more complex.
- **Negative:** Ongoing BigQuery query costs for heavy analytical workloads (mitigated by
  partition pruning and clustering).

---

## ADR-003 — MinIO retained as raw archive after BigQuery migration

**Date:** 2026-03-24
**Status:** Accepted

### Context

When migrating to BigQuery, a question arose: should MinIO (raw JSON storage) be removed
since BigQuery now holds the canonical dataset?

### Decision

Retain MinIO as the raw archive. BigQuery is the analytical store, not the source of truth.

### Consequences

- Raw JSON files in MinIO allow full re-derivation of any bronze table from scratch if
  schema changes, bugs in loaders, or BigQuery table drops require a backfill.
- Separation of concerns: ingestion (API → MinIO) is decoupled from loading (MinIO → BigQuery).
  Either leg can fail and be retried independently.
- Small operational cost: MinIO storage is local disk, not a paid service.

---

## ADR-004 — `interval` column name retained with backtick quoting in BigQuery

**Date:** 2026-03-24
**Status:** Accepted

### Context

The OpenF1 API returns a field named `interval` in the intervals endpoint (gap to the car
directly ahead). `INTERVAL` is a reserved keyword in BigQuery SQL.

### Decision

Retain the column name `interval` to match the source API, quoting it with backticks in DDL
and all queries. Renaming to `interval_to_car_ahead` was considered but rejected to keep
the schema consistent with the API response structure for easier debugging.

### Consequences

- All SQL referencing this column must use backtick quoting: `` `interval` ``.
- dbt models querying `bronze.intervals` must quote the column.
- Slightly non-standard but self-documenting via inline DDL comment.

---

## ADR-005 — Bronze loader deduplication via content-hash idempotency

**Date:** 2026-03-24
**Status:** Accepted

### Context

The bronze loaders use `WRITE_APPEND` load jobs. Without a guard, re-triggering
a DAG run for the same `meeting_key` (e.g. on retry or backfill) appends
duplicate rows to every table.

Three candidate strategies were evaluated:

1. **File-existence check** (`SELECT 1 WHERE _source_file = X`) — skips files
   already seen, but cannot detect when a source file is corrected/updated.
   A file that loaded successfully but was later updated in MinIO would never
   be reloaded.

2. **DELETE + re-append** (always delete before inserting) — guarantees clean
   state but runs a DELETE DML statement unconditionally on every run, even
   when nothing has changed.

3. **Content-hash + conditional DELETE** (chosen) — SHA-256 of raw MinIO bytes
   compared against `bronze.load_audit`. Skip if unchanged; DELETE then reload
   if the hash differs.

### Decision

Implement content-hash idempotency at the file level:

1. For each MinIO file, compute `SHA-256(raw_bytes)`.
2. Look up the stored hash in `bronze.load_audit` (keyed on `source_file`).
   - No record → first load; proceed.
   - Hash match → file unchanged; skip entirely (no BQ reads or writes).
   - Hash differs → source was corrected; DELETE existing rows, reload, update hash.
3. Record the hash in `bronze.load_audit` **after** a successful insert.
   If the insert fails, no hash is written and the next run retries cleanly.

**Special case — meetings file:**
`{year}/meetings/meetings.json` contains all Grand Prix events for the year.
When `meeting_key` is specified, the audit key is suffixed:
`"{source_file}#meeting={meeting_key}"`. This allows meeting 1262 and meeting 1263
to be loaded independently from the same file without one skipping the other.
The DELETE for a filtered meeting uses
`WHERE _source_file = X AND meeting_key = N` rather than deleting all rows
for the file.

**New table:** `bronze.load_audit` — one row per audit key:
`(source_file STRING, content_hash STRING, table_name STRING, row_count INT64, loaded_at TIMESTAMP)`
clustered on `source_file`.

### Consequences

- **Positive:** DAG retries and backfill re-runs are no-ops for unchanged files —
  no duplicate rows, no unnecessary BQ compute.
- **Positive:** Detects source corrections — if OpenF1 updates historical data
  and ingestion re-writes a MinIO file, the hash difference triggers a clean
  reload. A simple file-existence check would miss this silently.
- **Positive:** Full audit trail in `bronze.load_audit` — row counts, hashes,
  and timestamps are queryable for debugging and lineage.
- **Positive:** Partial-load safety — hash recorded only after successful insert;
  a mid-run crash leaves no hash record so the file is retried in full.
- **Negative:** Two extra BQ queries per file (hash lookup + hash write via MERGE).
  At F1 data volumes this is negligible.
- **Negative:** Hash is computed over the raw file bytes before JSON parsing,
  adding a small in-memory overhead for large files (car_data, positions,
  locations). Since the bytes are already read for parsing, no additional
  network I/O is required.
- **Negative:** Concurrent DAG runs loading the same file would both see no hash
  and both load. Mitigated by `max_active_runs=1` on the DAG.
