"""
Generate large deterministic airline-fare CSV test files.

This creates two files with the same schema as your sample CSV:

1. T0StreamingDirectly_10M_base.csv
   - 10,000,000 unique fare rows by default

2. T0StreamingDirectly_10M_100k_changes.csv
   - same 10,000,000 keys
   - exactly 100,000 rows have changed business fields by default

Usage from your project root:

    python generate_fare_test_csvs.py \
      --sample sample-data/T0StreamingDirectly.csv \
      --out-dir sample-data \
      --rows 10000000 \
      --changes 100000

For a quick smoke test:

    python generate_fare_test_csvs.py \
      --sample sample-data/T0StreamingDirectly.csv \
      --out-dir sample-data \
      --rows 1000 \
      --changes 100
"""

from __future__ import annotations

import argparse
import csv
import gzip
import os
import time
from pathlib import Path
from typing import Iterable


CARRIERS = ["DL", "AA", "UA", "WN", "AS", "B6", "NK", "F9"]
ROUTES = [
    ("GEG", "MCO"),
    ("GEG", "MLB"),
    ("OAK", "RSW"),
    ("HNL", "XWA"),
    ("LWS", "OGG"),
    ("KOA", "LWS"),
    ("MOT", "OGG"),
    ("SLC", "LAX"),
    ("SLC", "JFK"),
    ("ATL", "AMS"),
    ("DFW", "ORD"),
    ("SEA", "BOS"),
]
FARE_CLASSES = [
    "UA7ZA5ME",
    "BA01A0CL",
    "MA0LFFN",
    "V14NR",
    "Q21ADV",
    "YFULL",
    "K7SALE",
    "TWEB",
]
CURRENCIES = ["USD", "CAD", "EUR"]

SOURCE_FIELDS = {
    "action",
    "atpsource",
    "batchnumber",
    "key_column",
    "subdate",
    "subid",
    "subtime",
}

INTEGER_FIELDS = {
    "action",
    "batchnumber",
    "fare_tariff_number",
    "rule_tariff_number",
    "link_nbr",
    "link_seq_nbr",
    "ow_rt_ind",
    "cat5_adv_res_last",
    "cat5_adv_tktg",
    "cat5_ap",
    "cat5_res_hold",
    "cat6_unit_of_tm",
    "cat7_unit_of_tm",
    "fare_amount",
    "one_way_fare_amount",
}

FLOAT_FIELDS = {
    "untaxed_fare_amount",
    "fare_tax_amount",
    "fare_tax_rate",
}


class Progress:
    def __init__(self, total: int, interval: int = 1_000_000) -> None:
        self.total = total
        self.interval = interval
        self.started = time.time()
        self.last = 0

    def maybe_print(self, count: int) -> None:
        if count == self.total or count - self.last >= self.interval:
            elapsed = time.time() - self.started
            rate = count / elapsed if elapsed > 0 else 0
            pct = (count / self.total) * 100 if self.total else 0
            print(
                f"  wrote {count:,}/{self.total:,} rows ({pct:.1f}%) "
                f"at {rate:,.0f} rows/sec",
                flush=True,
            )
            self.last = count


def open_text(path: Path, gzip_output: bool):
    if gzip_output:
        return gzip.open(path, "wt", newline="", encoding="utf-8")
    return path.open("w", newline="", encoding="utf-8")


def read_template(sample_path: Path) -> tuple[list[str], dict[str, str]]:
    with sample_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"Sample CSV has no header: {sample_path}")
        try:
            row = next(reader)
        except StopIteration as exc:
            raise ValueError(f"Sample CSV has no data rows: {sample_path}") from exc
        return list(reader.fieldnames), dict(row)


def set_if_present(row: dict[str, str], key: str, value: object) -> None:
    if key in row:
        row[key] = str(value)


def build_row(template: dict[str, str], index: int, changed: bool, batchnumber: int) -> dict[str, str]:
    row = dict(template)

    carrier = CARRIERS[index % len(CARRIERS)]
    origin, destination = ROUTES[index % len(ROUTES)]
    fare_class = FARE_CLASSES[index % len(FARE_CLASSES)]
    currency = CURRENCIES[index % len(CURRENCIES)]
    market = f"{origin}{destination}"

    # Stable unique key. The changed file uses the exact same key values.
    source_key = f"T0FARE{index:010d}"

    # Deterministic base business values.
    base_fare = 100 + (index % 900)
    one_way = base_fare + 419 + (index % 13)
    untaxed = round(one_way * 0.93025, 2)
    tax_rate = 0.07500005
    tax = round(one_way - untaxed, 8)

    # Exactly the first N matching keys in the second file get business changes.
    if changed:
        base_fare += 25
        one_way += 25
        untaxed = round(one_way * 0.93025, 2)
        tax = round(one_way - untaxed, 8)

    set_if_present(row, "key_column", source_key)
    set_if_present(row, "carrier_code", carrier)
    set_if_present(row, "origin_city_code", origin)
    set_if_present(row, "destination_city_code", destination)
    set_if_present(row, "market", market)
    set_if_present(row, "market_real", market)
    set_if_present(row, "fare_class_code", fare_class)
    set_if_present(row, "currency_code", currency)

    set_if_present(row, "fare_amount", base_fare)
    set_if_present(row, "one_way_fare_amount", one_way)
    set_if_present(row, "untaxed_fare_amount", f"{untaxed:.2f}")
    set_if_present(row, "fare_tax_amount", f"{tax:.8f}")
    set_if_present(row, "fare_tax_rate", f"{tax_rate:.8f}")

    set_if_present(row, "batchnumber", batchnumber)
    set_if_present(row, "subid", 3200 + (index % 50))
    set_if_present(row, "subtime", f"{(index % 24):02d}00")
    set_if_present(row, "subdate", "27JAN26")
    set_if_present(row, "action", 3)

    set_if_present(row, "fare_tariff_number", index % 2500)
    set_if_present(row, "rule_tariff_number", 10 + (index % 90))
    set_if_present(row, "link_nbr", 1 + (index % 99))
    set_if_present(row, "link_seq_nbr", 1 + (index % 9999))
    set_if_present(row, "routing_number", 1000 + (index % 9000))
    set_if_present(row, "rule_number", f"R{index % 9999:04d}")

    # Keep record keys stable but varied. These are business-hashed rule fields.
    set_if_present(row, "record1_key", f"{carrier}RC{index % 9999:04d}{fare_class}1000000")
    for field in row:
        if field.startswith("record2_") and row[field] not in (None, ""):
            row[field] = f"{carrier}{field[-4:].upper()}{index % 999999:06d}"

    return row


def write_csv(
    *,
    output_path: Path,
    fieldnames: list[str],
    template: dict[str, str],
    rows: int,
    changes: int,
    changed_file: bool,
    batchnumber: int,
    gzip_output: bool,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    progress = Progress(total=rows)

    print(f"Writing {output_path}", flush=True)
    with open_text(output_path, gzip_output) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()

        for i in range(rows):
            row = build_row(
                template=template,
                index=i,
                changed=changed_file and i < changes,
                batchnumber=batchnumber,
            )
            writer.writerow(row)
            progress.maybe_print(i + 1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate large fare ETL CSV test files.")
    parser.add_argument("--sample", default="sample-data/T0StreamingDirectly.csv")
    parser.add_argument("--out-dir", default="sample-data")
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument("--changes", type=int, default=100_000)
    parser.add_argument("--base-name", default="T0StreamingDirectly_10M_base.csv")
    parser.add_argument("--changed-name", default="T0StreamingDirectly_10M_100k_changes.csv")
    parser.add_argument("--gzip", action="store_true", help="Write .gz-compressed CSV files.")

    args = parser.parse_args()

    if args.changes > args.rows:
        raise ValueError("--changes cannot be larger than --rows")

    sample_path = Path(args.sample)
    out_dir = Path(args.out_dir)

    fieldnames, template = read_template(sample_path)

    base_name = args.base_name + (".gz" if args.gzip and not args.base_name.endswith(".gz") else "")
    changed_name = args.changed_name + (".gz" if args.gzip and not args.changed_name.endswith(".gz") else "")

    base_path = out_dir / base_name
    changed_path = out_dir / changed_name

    print(f"Sample: {sample_path}")
    print(f"Columns: {len(fieldnames)}")
    print(f"Rows per file: {args.rows:,}")
    print(f"Changed rows in second file: {args.changes:,}")
    print("")

    write_csv(
        output_path=base_path,
        fieldnames=fieldnames,
        template=template,
        rows=args.rows,
        changes=args.changes,
        changed_file=False,
        batchnumber=90001,
        gzip_output=args.gzip,
    )

    write_csv(
        output_path=changed_path,
        fieldnames=fieldnames,
        template=template,
        rows=args.rows,
        changes=args.changes,
        changed_file=True,
        batchnumber=90002,
        gzip_output=args.gzip,
    )

    print("")
    print("Done.")
    print(f"Base file:    {base_path}")
    print(f"Changed file: {changed_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
