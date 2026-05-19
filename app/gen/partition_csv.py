from __future__ import annotations

import argparse
import csv
import time
import zlib
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Partition a large fare CSV into deterministic worker files."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV path.",
    )

    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory for partitioned CSV files.",
    )

    parser.add_argument(
        "--partitions",
        type=int,
        required=True,
        help="Number of output partitions/workers.",
    )

    parser.add_argument(
        "--key-column",
        default="key_column",
        help="Column used for deterministic partitioning.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=100000,
        help="Print progress every N rows.",
    )

    return parser.parse_args()


def partition_for_key(key: str, partitions: int) -> int:
    if not key:
        # Match the ETL behavior: bad/missing keys go to worker 0.
        return 0

    return zlib.crc32(str(key).encode("utf-8")) % partitions


def main() -> int:
    args = parse_args()

    if args.partitions < 1:
        raise ValueError("--partitions must be >= 1")

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file does not exist: {input_path}")

    started = time.perf_counter()

    output_paths = [
        out_dir / f"{input_path.stem}_part-{i:03d}-of-{args.partitions:03d}{input_path.suffix}"
        for i in range(args.partitions)
    ]

    print(f"Input: {input_path}")
    print(f"Output directory: {out_dir}")
    print(f"Partitions: {args.partitions}")

    writers: list[csv.DictWriter] = []
    handles = []
    counts = [0 for _ in range(args.partitions)]

    try:
        with input_path.open("r", newline="", encoding="utf-8-sig") as input_file:
            reader = csv.DictReader(input_file)

            if not reader.fieldnames:
                raise ValueError("Input CSV has no header row.")

            if args.key_column not in reader.fieldnames:
                raise ValueError(
                    f"Key column '{args.key_column}' not found in CSV header."
                )

            for output_path in output_paths:
                handle = output_path.open("w", newline="", encoding="utf-8")
                handles.append(handle)

                writer = csv.DictWriter(
                    handle,
                    fieldnames=reader.fieldnames,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writers.append(writer)

            total = 0

            for row in reader:
                total += 1

                key = row.get(args.key_column, "")
                partition = partition_for_key(key, args.partitions)

                writers[partition].writerow(row)
                counts[partition] += 1

                if total % args.progress_every == 0:
                    elapsed = time.perf_counter() - started
                    rps = total / elapsed if elapsed > 0 else 0

                    print(
                        f"[progress] rows={total:,} "
                        f"rps={rps:,.2f} "
                        f"counts={counts}",
                        flush=True,
                    )

    finally:
        for handle in handles:
            handle.close()

    elapsed = time.perf_counter() - started
    rps = sum(counts) / elapsed if elapsed > 0 else 0

    print("")
    print("Partition complete.")
    print(f"Total rows: {sum(counts):,}")
    print(f"Duration seconds: {elapsed:.3f}")
    print(f"Rows/sec: {rps:,.2f}")

    for i, path in enumerate(output_paths):
        print(f"Partition {i}: {counts[i]:,} rows -> {path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())