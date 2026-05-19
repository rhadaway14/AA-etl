from __future__ import annotations

import argparse
import sys

from dotenv import load_dotenv

from .config import CouchbaseConfig
from .processor import FareBatchProcessor, print_json


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process fare CSV into Couchbase current/history collections.")
    parser.add_argument("--csv", required=True, help="Path to the fare CSV file.")
    parser.add_argument("--batch-id", required=True, help="Deterministic batch id, e.g. batch::2026-05-18T15.")
    parser.add_argument("--dry-run", action="store_true", help="Parse and preview documents without connecting to Couchbase.")
    parser.add_argument("--preview", type=int, default=0, help="Number of rows to print in dry-run mode.")
    parser.add_argument("--env-file", default=None, help="Optional .env file path.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])

    if args.env_file:
        load_dotenv(args.env_file)
    else:
        load_dotenv()

    repo = None
    try:
        if not args.dry_run:
            from .couchbase_client import CouchbaseFareRepository

            config = CouchbaseConfig.from_env()
            repo = CouchbaseFareRepository(config)

        processor = FareBatchProcessor(repository=repo, dry_run=args.dry_run, preview=args.preview)
        stats = processor.process_csv(csv_path=args.csv, batch_id=args.batch_id)
        print("\n=== BATCH STATS ===")
        print_json({"type": "fare_batch", **stats.__dict__})
        return 0
    finally:
        if repo is not None:
            repo.close()


if __name__ == "__main__":
    raise SystemExit(main())
