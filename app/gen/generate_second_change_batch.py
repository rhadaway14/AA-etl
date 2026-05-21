from __future__ import annotations

import argparse
import csv
from pathlib import Path


PRICE_FIELDS = [
    "fare_amount",
    "fare_tax_amount",
    "one_way_fare_amount",
    "untaxed_fare_amount",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a second change batch from an existing fare CSV."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input CSV, usually T0StreamingDirectly_10M_100k_changes.csv",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path for the second change batch.",
    )

    parser.add_argument(
        "--changes",
        type=int,
        default=100000,
        help="Number of rows to change.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=500000,
    )

    return parser.parse_args()


def parse_number(value: str, fallback: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return fallback


def format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))

    return str(round(value, 2))


def mutate_row(row: dict[str, str], row_index: int) -> dict[str, str]:
    """
    Apply a second deterministic change.

    This intentionally changes price fields again so the same fare gets
    another history document.
    """
    mutated = dict(row)

    fare_amount = parse_number(mutated.get("fare_amount", "0"))
    fare_tax_amount = parse_number(mutated.get("fare_tax_amount", "0"))
    one_way_fare_amount = parse_number(mutated.get("one_way_fare_amount", "0"))
    untaxed_fare_amount = parse_number(mutated.get("untaxed_fare_amount", "0"))

    # Deterministic but slightly varied changes.
    bump = 25 + (row_index % 7)

    mutated["fare_amount"] = format_number(fare_amount + bump)
    mutated["fare_tax_amount"] = format_number(fare_tax_amount + round(bump * 0.075, 2))
    mutated["one_way_fare_amount"] = format_number(one_way_fare_amount + bump)
    mutated["untaxed_fare_amount"] = format_number(untaxed_fare_amount + bump)

    # Optional metadata changes so you can see this was a later batch.
    mutated["batchnumber"] = "90003"
    mutated["subtime"] = "0100"

    return mutated


def main() -> int:
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    scanned = 0
    changed = 0

    with input_path.open("r", newline="", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)

        if not reader.fieldnames:
            raise ValueError("Input CSV has no header row.")

        with output_path.open("w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                scanned += 1

                if changed < args.changes:
                    row = mutate_row(row, changed)
                    changed += 1

                writer.writerow(row)

                if scanned % args.progress_every == 0:
                    print(
                        f"[progress] scanned={scanned:,} changed={changed:,}",
                        flush=True,
                    )

    print("")
    print("Second change batch generated.")
    print(f"Input:   {input_path}")
    print(f"Output:  {output_path}")
    print(f"Scanned: {scanned:,}")
    print(f"Changed: {changed:,}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())