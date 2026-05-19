# Fare ETL 10M-row CSV generator

This creates two CSV files with the same schema as `sample-data/T0StreamingDirectly.csv`:

1. `T0StreamingDirectly_10M_base.csv`
   - 10,000,000 unique fare rows

2. `T0StreamingDirectly_10M_100k_changes.csv`
   - same 10,000,000 `key_column` values
   - exactly 100,000 rows have changed business fields (`fare_amount`, `one_way_fare_amount`, `untaxed_fare_amount`, `fare_tax_amount`)

## Install

Copy `generate_fare_test_csvs.py` into your project root:

```powershell
C:\Projects\Python\AA-etl\fare-etl
```

## Generate the full files

```powershell
cd C:\Projects\Python\AA-etl\fare-etl
.\.venv\Scripts\Activate.ps1

python generate_fare_test_csvs.py `
  --sample sample-data/T0StreamingDirectly.csv `
  --out-dir sample-data `
  --rows 10000000 `
  --changes 100000
```

This creates:

```text
sample-data/T0StreamingDirectly_10M_base.csv
sample-data/T0StreamingDirectly_10M_100k_changes.csv
```

## Quick smoke test first

```powershell
python generate_fare_test_csvs.py `
  --sample sample-data/T0StreamingDirectly.csv `
  --out-dir sample-data `
  --rows 1000 `
  --changes 100 `
  --base-name T0StreamingDirectly_1k_base.csv `
  --changed-name T0StreamingDirectly_1k_100_changes.csv
```

Then run:

```powershell
python -m app.main --csv sample-data/T0StreamingDirectly_1k_base.csv --batch-id batch::1k-base
python -m app.main --csv sample-data/T0StreamingDirectly_1k_100_changes.csv --batch-id batch::1k-100-changes
```

Expected second run:

```text
changed_count: 100
unchanged_count: 900
failed_count: 0
```

## Run the full 10M test

```powershell
python -m app.main --csv sample-data/T0StreamingDirectly_10M_base.csv --batch-id batch::10m-base
python -m app.main --csv sample-data/T0StreamingDirectly_10M_100k_changes.csv --batch-id batch::10m-100k-changes
```

Expected second run:

```text
changed_count: 100000
unchanged_count: 9900000
failed_count: 0
```

## Optional gzip output

The generator supports `--gzip`, but the current ETL reader expects normal CSV. Use gzip only if you add gzip support to `csv_reader.py` or decompress before running the ETL.
