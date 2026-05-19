# Fare ETL: CSV -> Couchbase Capella

This project reads the uploaded fare CSV format, normalizes each row, computes a deterministic business hash, compares it with the current Couchbase document, and writes only changed fares to a 7-day history collection.

## Couchbase layout

```text
Bucket: fares
  Scope: airline
    Collection: current_fares
    Collection: batch_control
    Collection: dead_letter

Bucket: fare_history
  Scope: airline
    Collection: fare_changes_7d   # collection TTL recommended: 691200 seconds / 8 days
```

## Run locally in dry-run mode

```bash
python -m venv .venv
source .venv/bin/activate  # Windows Git Bash
pip install -r requirements.txt
python -m app.main --csv sample-data/T0StreamingDirectly.csv --batch-id batch::local-test-001 --dry-run --preview 3
```

## Run locally against Capella

```bash
cp .env.example .env
# edit .env
set -a; source .env; set +a
python -m app.main --csv sample-data/T0StreamingDirectly.csv --batch-id batch::local-test-001
```

## Docker dry-run

```bash
docker build -t fare-etl:latest .
docker run --rm -v "$PWD/sample-data:/data" fare-etl:latest \
  --csv /data/T0StreamingDirectly.csv \
  --batch-id batch::docker-test-001 \
  --dry-run \
  --preview 3
```

## Docker against Capella

```bash
docker run --rm --env-file .env -v "$PWD/sample-data:/data" fare-etl:latest \
  --csv /data/T0StreamingDirectly.csv \
  --batch-id batch::docker-test-001
```

python -m app.main --csv sample-data/T0StreamingDirectly.csv --batch-id batch::local-test-001 --dry-run --preview 1

python -m app.main --csv sample-data/T0StreamingDirectly.csv --batch-id batch::local-test-001