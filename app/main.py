from __future__ import annotations

import argparse
import asyncio

from .config import CouchbaseConfig
from .couchbase_client import AsyncCouchbaseFareRepository
from .processor import AsyncFareBatchProcessor, print_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process airline fare CSV batches into Couchbase."
    )

    parser.add_argument(
        "--csv",
        required=True,
        help="Path to the CSV file to process.",
    )

    parser.add_argument(
        "--batch-id",
        required=True,
        help="Unique batch id, for example batch::10m-base.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and build documents without writing to Couchbase.",
    )

    parser.add_argument(
        "--preview",
        type=int,
        default=0,
        help="In dry-run mode, print the first N generated docs.",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=1000,
        help="Maximum number of in-flight row processing tasks per worker.",
    )

    parser.add_argument(
        "--progress-every",
        type=int,
        default=10000,
        help="Print progress every N scanned rows.",
    )

    parser.add_argument(
        "--batch-control-every",
        type=int,
        default=100000,
        help="Update batch_control every N scanned rows.",
    )

    parser.add_argument(
        "--skip-new-history",
        action="store_true",
        help=(
            "Do not write NEW_FARE history events for new rows. "
            "Useful for faster baseline seeding."
        ),
    )

    parser.add_argument(
        "--initial-load",
        action="store_true",
        help=(
            "Fast baseline mode. Upserts current documents without reading existing "
            "documents and without writing NEW_FARE history."
        ),
    )

    parser.add_argument(
        "--max-cas-retries",
        type=int,
        default=3,
        help="Maximum CAS retry attempts for concurrent updates.",
    )

    parser.add_argument(
        "--worker-count",
        type=int,
        default=1,
        help="Total number of parallel workers.",
    )

    parser.add_argument(
        "--worker-id",
        type=int,
        default=0,
        help="This worker's zero-based worker id.",
    )

    return parser.parse_args()


async def async_main() -> int:
    args = parse_args()

    if args.worker_count < 1:
        raise ValueError("--worker-count must be >= 1")

    if args.worker_id < 0 or args.worker_id >= args.worker_count:
        raise ValueError("--worker-id must be between 0 and worker-count - 1")

    repo = None

    try:
        if not args.dry_run:
            config = CouchbaseConfig.from_env()
            repo = AsyncCouchbaseFareRepository(config)
            await repo.connect()

        processor = AsyncFareBatchProcessor(
            repository=repo,
            dry_run=args.dry_run,
            preview=args.preview,
            concurrency=args.concurrency,
            progress_every=args.progress_every,
            batch_control_every=args.batch_control_every,
            max_cas_retries=args.max_cas_retries,
            skip_new_history=args.skip_new_history,
            initial_load=args.initial_load,
            worker_count=args.worker_count,
            worker_id=args.worker_id,
        )

        stats = await processor.process_csv(
            csv_path=args.csv,
            batch_id=args.batch_id,
        )

        print("\n=== WORKER STATS ===")
        print_json(
            {
                "type": "fare_batch_worker",
                **stats.__dict__,
            }
        )

        return 0

    finally:
        if repo is not None:
            await repo.close()


def main() -> int:
    return asyncio.run(async_main())


if __name__ == "__main__":
    raise SystemExit(main())