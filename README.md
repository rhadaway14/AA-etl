# Fare ETL: CSV → Couchbase Capella

This project ingests airline fare CSV batches, normalizes each row into a Couchbase document, computes a deterministic business hash, compares incoming rows against existing fare state, and stores only meaningful changes in a 7-day history collection.

The design is optimized for large hourly uploads where the source CSV may contain millions of rows, but only a subset of fares actually change.

---

## Goals

This ETL supports:

- Loading an initial baseline of fare documents.
- Processing large recurring CSV batches.
- Detecting unchanged fares efficiently using a compact hash collection.
- Updating only changed fare documents.
- Writing sparse change history for point-in-time reconstruction.
- Tracking batch progress and performance.
- Capturing failed records in a dead-letter collection.
- Running in parallel using partitioned CSV files and multiple workers.

---

## Couchbase Layout

```text
Bucket: fares
  Scope: airline
    Collection: current_fares
    Collection: fare_hashes
    Collection: batch_control
    Collection: dead_letter

Bucket: fare_history
  Scope: airline
    Collection: fare_changes_7d
```

Recommended TTL for `fare_history.airline.fare_changes_7d`:

```text
691200 seconds
8 days
```

The business requirement is 7 days of history. Using 8 days gives a safety buffer for clock drift, delayed jobs, and operational review.

---

# Data Models

## 1. Current Fare Document

Collection:

```text
fares.airline.current_fares
```

Document key format:

```text
fare::<source_key>
```

Example:

```text
fare::T0FARE0000000000
```

Purpose:

The current fare document stores the latest known state for a fare.

Example document:

```json
{
  "type": "fare_current",
  "fare_key": "fare::T0FARE0000000000",
  "source_key": "T0FARE0000000000",
  "business_hash": "sha256:3fef213bc7551911993d67fd3fa4a8dce092255365c07fc5785d2e4f3647e264",
  "fare_data": {
    "carrier_code": "DL",
    "origin_city_code": "GEG",
    "destination_city_code": "MCO",
    "market": "GEGMCO",
    "market_real": "GEGMCO",
    "fare_class_code": "UA7ZA5ME",
    "fare_amount": 125,
    "fare_tax_amount": 37.94,
    "fare_tax_rate": 0.07500005,
    "one_way_fare_amount": 544.0,
    "untaxed_fare_amount": 506.06,
    "currency_code": "USD",
    "fare_expiry_status": "ACTIVE",
    "passenger_type": "00",
    "routing_number": "1000",
    "ticket_first": "00XXX00",
    "ticket_last": "00XXX00",
    "effective_date": "05MAY26",
    "discontinue_date": "00XXX00"
  },
  "rule": {
    "record0_key": "null",
    "record1_key": "DLRC0000UA7ZA5ME1000000",
    "record2_frakey_cat2": null,
    "record2_frakey_cat3": null,
    "record2_frakey_cat5": null,
    "record2_frakey_cat6": null,
    "record2_frakey_cat7": null,
    "record2_frakey_cat11": null,
    "record2_frakey_cat14": null,
    "record2_frakey_cat15": null,
    "record2_frakey_cat16": null
  },
  "source": {
    "last_batch_id": "batch::10m-changes-tiny-hash-w4-c2000",
    "source_file": "T0StreamingDirectly_10M_100k_changes_part-000-of-004.csv",
    "row_number": 1,
    "action": 3,
    "atpsource": "ATP",
    "batchnumber": 90002,
    "subdate": "27JAN26",
    "subid": "3200",
    "subtime": "0000"
  },
  "created_at": "2026-05-20T07:02:51Z",
  "updated_at": "2026-05-20T08:12:46Z"
}
```

Important fields:

| Field | Purpose |
|---|---|
| `fare_key` | Couchbase document key, also stored in the body for query/debug convenience. |
| `source_key` | Original fare identifier from the CSV, usually from `key_column`. |
| `business_hash` | Deterministic hash of business-relevant fare fields. Used to detect changes. |
| `fare_data` | Normalized fare attributes from the CSV. |
| `rule` | Rule/category-related fields from the CSV. |
| `source` | Metadata about the CSV batch and row that last updated this document. |
| `created_at` | First time this fare was inserted into Couchbase. |
| `updated_at` | Last time this fare changed. |

---

## 2. Compact Fare Hash Document

Collection:

```text
fares.airline.fare_hashes
```

Document key format:

```text
hash::fare::<source_key>
```

Example:

```text
hash::fare::T0FARE0000000000
```

Purpose:

This collection is the high-performance comparison layer. For recurring full-snapshot CSV uploads, the ETL reads this tiny document first. If the hash matches, the row is unchanged and the ETL skips expensive document construction and current-document reads.

Current compact format:

```json
{
  "h": "sha256:3fef213bc7551911993d67fd3fa4a8dce092255365c07fc5785d2e4f3647e264"
}
```

Previous verbose format, still supported for backward compatibility:

```json
{
  "type": "fare_hash",
  "fare_key": "fare::T0FARE0000000000",
  "source_key": "T0FARE0000000000",
  "business_hash": "sha256:3fef213bc7551911993d67fd3fa4a8dce092255365c07fc5785d2e4f3647e264",
  "last_batch_id": "batch::example",
  "updated_at": "2026-05-20T08:12:46Z"
}
```

Important fields:

| Field | Purpose |
|---|---|
| `h` | Compact business hash value. |

Why this exists:

Reading 10 million full fare documents just to determine whether they changed is expensive. Reading 10 million tiny hash documents is much cheaper.

Optimized comparison path:

```text
CSV row
  → derive fare_key and business_hash
  → GET fare_hashes hash doc
  → if hash matches: skip
  → if hash differs: build full doc, update current_fares, write history, update hash
```

---

## 3. Fare Change History Document

Collection:

```text
fare_history.airline.fare_changes_7d
```

Document key format:

```text
change::<source_key>::<batch_id>::<business_hash>
```

Example:

```text
change::T0FARE0000000000::batch::10m-changes-tiny-hash-w4-c2000::sha256-abc123
```

Purpose:

Stores sparse field-level changes for point-in-time reconstruction.

Example document:

```json
{
  "type": "fare_change",
  "fare_key": "fare::T0FARE0000000000",
  "source_key": "T0FARE0000000000",
  "batch_id": "batch::10m-changes-tiny-hash-w4-c2000",
  "change_type": "UPDATED_FARE",
  "changed_at": "2026-05-20T08:12:46Z",
  "changed_fields": [
    "fare_data.fare_amount",
    "fare_data.fare_tax_amount",
    "fare_data.one_way_fare_amount",
    "fare_data.untaxed_fare_amount"
  ],
  "diff": {
    "fare_data.fare_amount": {
      "old": 100,
      "new": 125
    },
    "fare_data.fare_tax_amount": {
      "old": 36.2,
      "new": 37.94
    },
    "fare_data.one_way_fare_amount": {
      "old": 519.0,
      "new": 544.0
    },
    "fare_data.untaxed_fare_amount": {
      "old": 482.8,
      "new": 506.06
    }
  },
  "source": {
    "source_file": "T0StreamingDirectly_10M_100k_changes_part-000-of-004.csv",
    "row_number": 1,
    "action": 3,
    "atpsource": "ATP",
    "batchnumber": 90002,
    "subdate": "27JAN26",
    "subid": "3200",
    "subtime": "0000"
  }
}
```

Important fields:

| Field | Purpose |
|---|---|
| `fare_key` | Current fare document key. |
| `source_key` | Original source fare key. |
| `batch_id` | Batch that created the history record. |
| `change_type` | Usually `UPDATED_FARE` or `NEW_FARE`. |
| `changed_at` | Timestamp of the change. |
| `changed_fields` | List of fields that changed. |
| `diff` | Sparse old/new values for each changed field. |
| `source` | CSV source metadata. |

The history document intentionally stores only changed fields, not the entire document. This reduces storage cost and improves write performance.

Point-in-time reconstruction works by:

```text
1. Load the current fare document.
2. Find all history records after the requested timestamp.
3. Apply each diff.old value back onto the current document.
4. Oldest applicable value wins if multiple changes touched the same field.
```

---

## 4. Batch Control Document

Collection:

```text
fares.airline.batch_control
```

Document key format:

```text
<batch_id>
```

Example:

```text
batch::10m-changes-tiny-hash-w4-c2000
```

Purpose:

Tracks progress, worker stats, aggregate throughput, and final status for each ETL batch.

Example document:

```json
{
  "type": "fare_batch",
  "batch_id": "batch::10m-changes-tiny-hash-w4-c2000",
  "worker_count": 4,
  "status": "COMPLETED",
  "started_at": "2026-05-20T08:12:46Z",
  "completed_at": "2026-05-20T08:48:50Z",
  "workers": {
    "0": {
      "batch_id": "batch::10m-changes-tiny-hash-w4-c2000",
      "source_file": "T0StreamingDirectly_10M_100k_changes_part-000-of-004.csv",
      "status": "COMPLETED",
      "worker_count": 4,
      "worker_id": 0,
      "input_partition": true,
      "scanned_count": 2500000,
      "skipped_by_worker_count": 0,
      "received_count": 2500000,
      "processed_count": 2500000,
      "inserted_count": 0,
      "changed_count": 25000,
      "unchanged_count": 2475000,
      "failed_count": 0,
      "hash_match_count": 2475000,
      "hash_miss_count": 0,
      "hash_changed_count": 25000,
      "hash_upsert_count": 25000,
      "current_doc_build_count": 25000,
      "duration_seconds": 2147.997,
      "records_per_second": 1163.88
    }
  },
  "totals": {
    "scanned_count": 10000000,
    "skipped_by_worker_count": 0,
    "received_count": 10000000,
    "processed_count": 10000000,
    "inserted_count": 0,
    "changed_count": 100000,
    "unchanged_count": 9900000,
    "failed_count": 0,
    "hash_match_count": 9900000,
    "hash_miss_count": 0,
    "hash_changed_count": 100000,
    "hash_upsert_count": 100000,
    "current_doc_build_count": 100000,
    "wall_clock_seconds_estimate": 2164.192,
    "aggregate_records_per_second": 4620.66
  }
}
```

Important batch metrics:

| Metric | Meaning |
|---|---|
| `scanned_count` | Number of CSV rows read. |
| `skipped_by_worker_count` | Rows skipped because they belonged to another worker. Should be `0` when using `--input-partition`. |
| `received_count` | Rows assigned to this worker. |
| `processed_count` | Rows successfully processed. |
| `inserted_count` | New current fare documents inserted. |
| `changed_count` | Existing fare documents updated. |
| `unchanged_count` | Rows skipped because business hash matched. |
| `failed_count` | Rows written to dead letter. |
| `hash_match_count` | Rows where compact hash matched existing hash. |
| `hash_miss_count` | Rows where hash document did not exist. |
| `hash_changed_count` | Rows where hash existed but differed. |
| `hash_upsert_count` | Hash documents updated. |
| `current_doc_build_count` | Number of full current fare documents built. In optimized change runs, this should be close to `changed_count`, not total rows. |
| `aggregate_records_per_second` | Overall batch throughput estimate. |

---

## 5. Dead Letter Document

Collection:

```text
fares.airline.dead_letter
```

Document key format:

```text
deadletter::<batch_id>::worker-<worker_id>::<row_number>
```

Example:

```text
deadletter::batch::10m-test::worker-0::12345
```

Purpose:

Stores rows that failed parsing, transformation, hashing, or Couchbase write operations.

Example document:

```json
{
  "type": "dead_letter",
  "batch_id": "batch::10m-test",
  "source_file": "T0StreamingDirectly_10M_100k_changes_part-000-of-004.csv",
  "row_number": 12345,
  "worker_id": 0,
  "worker_count": 4,
  "input_partition": true,
  "error": "Example error message",
  "traceback": "Python traceback...",
  "raw_record": {
    "key_column": "T0FARE0000000000",
    "carrier_code": "DL",
    "fare_amount": "125"
  },
  "created_at": "2026-05-20T08:12:46Z"
}
```

Important fields:

| Field | Purpose |
|---|---|
| `batch_id` | Batch where the failure occurred. |
| `source_file` | CSV file being processed. |
| `row_number` | Source row number. |
| `worker_id` | Worker that encountered the error. |
| `error` | Short error message. |
| `traceback` | Python traceback for debugging. |
| `raw_record` | Original CSV row as parsed. |
| `created_at` | Time the dead-letter document was written. |

---

# Processing Modes

## Dry Run

Parses rows and builds documents without writing to Couchbase.

```bash
python -m app.main \
  --csv sample-data/T0StreamingDirectly.csv \
  --batch-id batch::local-test-001 \
  --dry-run \
  --preview 3
```

---

## Initial Load

Used when loading a clean baseline.

Behavior:

```text
For each row:
  build current_fares document
  upsert current_fares
  upsert compact fare_hashes document
  do not write NEW_FARE history
```

Command:

```bash
python -m app.main \
  --csv sample-data/T0StreamingDirectly.csv \
  --batch-id batch::initial-load-001 \
  --initial-load
```

---

## Normal Change Processing

Used for recurring CSV uploads.

Behavior:

```text
For each row:
  derive fare_key and business_hash
  read compact fare_hashes document
  if hash matches:
      mark unchanged
      skip
  if hash differs or is missing:
      build full current_fares document
      get existing current_fares document
      write sparse history diff
      update current_fares
      update fare_hashes
```

Command:

```bash
python -m app.main \
  --csv sample-data/T0StreamingDirectly.csv \
  --batch-id batch::change-upload-001
```

---

## Hash Backfill

Used to rebuild or repair the `fare_hashes` collection from a CSV.

Behavior:

```text
For each row:
  derive fare_key and business_hash
  upsert compact hash document only
```

Command:

```bash
python -m app.main \
  --csv sample-data/T0StreamingDirectly.csv \
  --batch-id batch::hash-backfill-001 \
  --hash-backfill
```

---

## Partitioned Input Mode

Use this when the CSV file has already been partitioned for a worker.

Without this flag, each worker applies:

```text
crc32(key_column) % worker_count == worker_id
```

With `--input-partition`, the worker processes every row in the file.

Command:

```bash
python -m app.main \
  --csv /tmp/aa-etl-partitioned/changes-4/T0StreamingDirectly_10M_100k_changes_part-000-of-004.csv \
  --batch-id batch::partitioned-change-001 \
  --worker-count 4 \
  --worker-id 0 \
  --input-partition
```

---

# Recommended Indexes

## History lookup by fare and timestamp

```sql
CREATE INDEX ix_fare_history_fare_key_changed_at
ON `fare_history`.`airline`.`fare_changes_7d`(fare_key, changed_at);
```

This index supports API queries such as:

```sql
SELECT *
FROM `fare_history`.`airline`.`fare_changes_7d`
WHERE fare_key = "fare::T0FARE0000000000"
  AND changed_at BETWEEN "2026-05-20T07:00:00Z" AND "2026-05-20T09:00:00Z";
```

---

# Run Locally in Dry-Run Mode

## Linux / Git Bash

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python -m app.main \
  --csv sample-data/T0StreamingDirectly.csv \
  --batch-id batch::local-test-001 \
  --dry-run \
  --preview 3
```

## PowerShell

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt

python -m app.main `
  --csv sample-data/T0StreamingDirectly.csv `
  --batch-id batch::local-test-001 `
  --dry-run `
  --preview 3
```

---

# Run Locally Against Capella

Create `.env`:

```bash
cp .env.example .env
```

Example `.env`:

```env
COUCHBASE_CONNSTR=couchbases://your-capella-endpoint.cloud.couchbase.com
COUCHBASE_USERNAME=your_user
COUCHBASE_PASSWORD=your_password

COUCHBASE_BUCKET=fares
COUCHBASE_SCOPE=airline
COUCHBASE_CURRENT_COLLECTION=current_fares
COUCHBASE_HASH_COLLECTION=fare_hashes
COUCHBASE_BATCH_COLLECTION=batch_control
COUCHBASE_DEADLETTER_COLLECTION=dead_letter

COUCHBASE_HISTORY_BUCKET=fare_history
COUCHBASE_HISTORY_SCOPE=airline
COUCHBASE_HISTORY_COLLECTION=fare_changes_7d
```

Linux / Git Bash:

```bash
set -a
source .env
set +a

python -m app.main \
  --csv sample-data/T0StreamingDirectly.csv \
  --batch-id batch::local-test-001
```

PowerShell:

```powershell
python -m app.main `
  --csv sample-data/T0StreamingDirectly.csv `
  --batch-id batch::local-test-001
```

---

# Docker Dry Run

```bash
docker build -t fare-etl:latest .

docker run --rm \
  -v "$PWD/sample-data:/data" \
  fare-etl:latest \
  --csv /data/T0StreamingDirectly.csv \
  --batch-id batch::docker-test-001 \
  --dry-run \
  --preview 3
```

---

# Docker Against Capella

```bash
docker run --rm \
  --env-file .env \
  -v "$PWD/sample-data:/data" \
  fare-etl:latest \
  --csv /data/T0StreamingDirectly.csv \
  --batch-id batch::docker-test-001
```

---

# Partitioning Large CSV Files

For large files, partition the CSV first so each worker reads only its assigned rows.

Example: split into 4 partition files.

```bash
python app/gen/partition_csv.py \
  --input /opt/AA-etl/sample-data/T0StreamingDirectly_10M_100k_changes.csv \
  --out-dir /tmp/aa-etl-partitioned/changes-4 \
  --partitions 4 \
  --progress-every 500000
```

Run 4 workers:

```bash
for i in 0 1 2 3; do
  python -m app.main \
    --csv /tmp/aa-etl-partitioned/changes-4/T0StreamingDirectly_10M_100k_changes_part-$(printf "%03d" "$i")-of-004.csv \
    --batch-id batch::10m-changes-input-partition-w4-c2000 \
    --worker-count 4 \
    --worker-id "$i" \
    --input-partition \
    --concurrency 2000 \
    --chunk-concurrency 2 \
    --progress-every 100000 \
    --batch-control-every 250000 \
    > /tmp/aa-etl-logs/changes-input-partition-worker-"$i".log 2>&1 &
done

wait
```

Expected batch metrics:

```json
{
  "scanned_count": 10000000,
  "skipped_by_worker_count": 0,
  "received_count": 10000000,
  "changed_count": 100000,
  "unchanged_count": 9900000,
  "failed_count": 0,
  "hash_match_count": 9900000,
  "hash_changed_count": 100000,
  "current_doc_build_count": 100000
}
```

---

# Performance Notes

The optimized full-snapshot change path is:

```text
CSV row
  → derive fare_key and business_hash
  → read compact hash document
  → skip unchanged rows
  → build full current document only for changed rows
  → write sparse history only for changed rows
```

For a 10M-row CSV with 100k changed rows, the optimized metrics should look like:

```text
received_count: 10,000,000
changed_count: 100,000
unchanged_count: 9,900,000
hash_match_count: 9,900,000
hash_changed_count: 100,000
current_doc_build_count: 100,000
failed_count: 0
```

If `current_doc_build_count` is close to 10M, lazy construction is not working.

If `hash_miss_count` is high, the `fare_hashes` collection is incomplete and should be rebuilt with `--hash-backfill`.

---

# Related API Project

A separate FastAPI project can query this data model.

Useful API endpoints:

```text
GET /fares/{fare_key}/current
GET /fares/{fare_key}/current-price
GET /fares/{fare_key}/as-of?as_of=<timestamp>
GET /fares/{fare_key}/as-of-price?as_of=<timestamp>
GET /fares/{fare_key}/history?after=<timestamp>&before=<timestamp>
GET /fares/{fare_key}/timeline?after=<timestamp>&before=<timestamp>
```

Point-in-time reconstruction uses:

```text
current_fares
+
fare_changes_7d.diff.old values after the requested timestamp
```

to rebuild the fare state as of a specific date/time.
