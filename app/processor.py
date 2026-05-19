from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
import json
import time
import traceback
import zlib

from .csv_reader import iter_csv_rows
from .fare_model import (
    build_current_document,
    build_history_document,
    history_key,
    row_to_parts,
    utc_now_iso,
)


@dataclass
class BatchStats:
    batch_id: str
    source_file: str
    status: str = "PROCESSING"

    worker_count: int = 1
    worker_id: int = 0

    scanned_count: int = 0
    skipped_by_worker_count: int = 0

    received_count: int = 0
    processed_count: int = 0
    inserted_count: int = 0
    changed_count: int = 0
    unchanged_count: int = 0
    failed_count: int = 0
    cas_retry_count: int = 0
    history_duplicate_count: int = 0
    skipped_new_history_count: int = 0
    hash_match_count: int = 0
    hash_miss_count: int = 0
    hash_changed_count: int = 0
    hash_upsert_count: int = 0

    started_at: str = ""
    completed_at: str | None = None

    duration_seconds: float | None = None
    duration_ms: int | None = None
    records_per_second: float | None = None
    scanned_records_per_second: float | None = None
    completed_count: int = 0
    in_flight_count: int = 0


class AsyncFareBatchProcessor:
    def __init__(
        self,
        repository: Any,
        *,
        dry_run: bool = False,
        preview: int = 0,
        concurrency: int = 1000,
        progress_every: int = 10000,
        batch_control_every: int = 100000,
        max_cas_retries: int = 3,
        skip_new_history: bool = False,
        initial_load: bool = False,
        worker_count: int = 1,
        worker_id: int = 0,
    ):
        self.repository = repository
        self.dry_run = dry_run
        self.preview = preview
        self.concurrency = concurrency
        self.progress_every = progress_every
        self.batch_control_every = batch_control_every
        self.max_cas_retries = max_cas_retries
        self.skip_new_history = skip_new_history
        self.initial_load = initial_load
        self.worker_count = worker_count
        self.worker_id = worker_id

        self._stats_lock = asyncio.Lock()

    async def process_csv(self, csv_path: str, batch_id: str) -> BatchStats:
        source_file = Path(csv_path).name
        started_perf = time.perf_counter()

        stats = BatchStats(
            batch_id=batch_id,
            source_file=source_file,
            worker_count=self.worker_count,
            worker_id=self.worker_id,
            started_at=utc_now_iso(),
        )

        await self._write_batch_control(stats)

        pending: set[asyncio.Task] = set()

        for row_number, row in iter_csv_rows(csv_path):
            async with self._stats_lock:
                stats.scanned_count += 1
                scanned = stats.scanned_count

            if not self._row_belongs_to_worker(row):
                async with self._stats_lock:
                    stats.skipped_by_worker_count += 1
                continue

            async with self._stats_lock:
                stats.received_count += 1

            task = asyncio.create_task(
                self._process_row_safely(
                    row=row,
                    row_number=row_number,
                    source_file=source_file,
                    batch_id=batch_id,
                    stats=stats,
                )
            )

            pending.add(task)

            if len(pending) >= self.concurrency:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for done_task in done:
                    await done_task

            if scanned % self.progress_every == 0:
                await self._print_progress(stats, started_perf)

            if scanned % self.batch_control_every == 0:
                self._update_runtime_metrics(stats, started_perf)
                await self._write_batch_control(stats)

        if pending:
            done, _ = await asyncio.wait(pending)
            for done_task in done:
                await done_task

        stats.status = "COMPLETED" if stats.failed_count == 0 else "COMPLETED_WITH_ERRORS"
        stats.completed_at = utc_now_iso()
        self._update_runtime_metrics(stats, started_perf)

        await self._write_batch_control(stats)
        await self._print_progress(stats, started_perf, final=True)

        return stats

    def _row_belongs_to_worker(self, row: dict[str, Any]) -> bool:
        if self.worker_count <= 1:
            return True

        key = row.get("key_column")

        if not key:
            return self.worker_id == 0

        partition = zlib.crc32(str(key).encode("utf-8")) % self.worker_count
        return partition == self.worker_id

    @staticmethod
    def _hash_key_for_fare_key(fare_key: str) -> str:
        return f"hash::{fare_key}"

    async def _process_row_safely(
        self,
        *,
        row: dict[str, Any],
        row_number: int,
        source_file: str,
        batch_id: str,
        stats: BatchStats,
    ) -> None:
        try:
            result = await self._process_row(
                row=row,
                row_number=row_number,
                source_file=source_file,
                batch_id=batch_id,
                stats=stats,
            )

            async with self._stats_lock:
                stats.processed_count += 1

                if result == "inserted":
                    stats.inserted_count += 1
                elif result == "changed":
                    stats.changed_count += 1
                elif result == "unchanged":
                    stats.unchanged_count += 1
                elif result == "skipped_new_history":
                    stats.inserted_count += 1
                    stats.skipped_new_history_count += 1

        except Exception as exc:
            async with self._stats_lock:
                stats.failed_count += 1

            print("")
            print("=" * 80, flush=True)
            print(f"[error] Failed processing row {row_number}", flush=True)
            print(f"[error] {type(exc).__name__}: {exc}", flush=True)
            print(traceback.format_exc(limit=8), flush=True)
            print("=" * 80, flush=True)
            print("")

            await self._write_deadletter(
                batch_id=batch_id,
                source_file=source_file,
                row_number=row_number,
                row=row,
                error=str(exc),
                traceback_text=traceback.format_exc(limit=8),
            )

    async def _process_row(
        self,
        *,
        row: dict[str, Any],
        row_number: int,
        source_file: str,
        batch_id: str,
        stats: BatchStats,
    ) -> str:
        now = utc_now_iso()

        parts = row_to_parts(
            row=row,
            batch_id=batch_id,
            source_file=source_file,
            row_number=row_number,
        )

        new_doc = build_current_document(parts=parts, now=now)
        hash_key = self._hash_key_for_fare_key(parts.fare_key)

        hash_doc = {
            "type": "fare_hash",
            "fare_key": parts.fare_key,
            "source_key": parts.source_key,
            "business_hash": parts.business_hash,
            "last_batch_id": batch_id,
            "updated_at": now,
        }

        if self.dry_run:
            if row_number <= self.preview:
                history_doc = build_history_document(
                    parts=parts,
                    change_type="NEW_FARE",
                    old_doc=None,
                    new_doc=new_doc,
                    batch_id=batch_id,
                    now=now,
                )

                print("\n--- DRY RUN CURRENT DOC ---")
                print_json(new_doc)

                print("\n--- DRY RUN HASH DOC ---")
                print_json(hash_doc)

                print("\n--- DRY RUN HISTORY DOC ---")
                print_json(history_doc)

            return "inserted"

        if self.initial_load:
            await self.repository.upsert_current(parts.fare_key, new_doc)
            await self.repository.upsert_hash(hash_key, hash_doc)

            async with self._stats_lock:
                stats.hash_upsert_count += 1

            return "skipped_new_history"

        existing_hash_doc = await self.repository.get_hash(hash_key)

        if existing_hash_doc is not None:
            existing_hash = existing_hash_doc.get("business_hash")

            if existing_hash == parts.business_hash:
                async with self._stats_lock:
                    stats.hash_match_count += 1
                return "unchanged"

            async with self._stats_lock:
                stats.hash_changed_count += 1
        else:
            async with self._stats_lock:
                stats.hash_miss_count += 1

        for _attempt in range(self.max_cas_retries + 1):
            existing = await self.repository.get_current(parts.fare_key)

            if existing is None:
                if not self.skip_new_history:
                    history_doc = build_history_document(
                        parts=parts,
                        change_type="NEW_FARE",
                        old_doc=None,
                        new_doc=new_doc,
                        batch_id=batch_id,
                        now=now,
                    )

                    await self.repository.insert_history(
                        history_key(parts, batch_id),
                        history_doc,
                    )

                inserted_current = await self.repository.insert_current(
                    parts.fare_key,
                    new_doc,
                )

                if inserted_current:
                    await self.repository.upsert_hash(hash_key, hash_doc)
                    async with self._stats_lock:
                        stats.hash_upsert_count += 1

                    if self.skip_new_history:
                        return "skipped_new_history"
                    return "inserted"

                continue

            old_doc, cas = self._coerce_existing_doc(existing)
            old_hash = old_doc.get("business_hash")

            if old_hash == parts.business_hash:
                await self.repository.upsert_hash(hash_key, hash_doc)
                async with self._stats_lock:
                    stats.hash_upsert_count += 1
                return "unchanged"

            updated_doc = build_current_document(
                parts=parts,
                now=now,
                existing_created_at=old_doc.get("created_at"),
            )

            history_doc = build_history_document(
                parts=parts,
                change_type="UPDATED_FARE",
                old_doc=old_doc,
                new_doc=updated_doc,
                batch_id=batch_id,
                now=now,
            )

            await self.repository.insert_history(
                history_key(parts, batch_id),
                history_doc,
            )

            replaced = await self.repository.replace_current(
                parts.fare_key,
                updated_doc,
                cas=cas,
            )

            if replaced:
                await self.repository.upsert_hash(hash_key, hash_doc)
                async with self._stats_lock:
                    stats.hash_upsert_count += 1
                return "changed"

        raise RuntimeError(f"CAS retries exceeded for {parts.fare_key}")

    @staticmethod
    def _coerce_existing_doc(
        current_result: Any,
    ) -> tuple[dict[str, Any], Any]:
        cas = None

        if isinstance(current_result, tuple) and len(current_result) == 2:
            old_doc, cas = current_result

        elif isinstance(current_result, dict) and "content" in current_result:
            old_doc = current_result.get("content")
            cas = current_result.get("cas")

        else:
            old_doc = current_result

        if isinstance(old_doc, str):
            old_doc = json.loads(old_doc)

        if not isinstance(old_doc, dict):
            raise TypeError(
                f"Expected existing current fare document to be dict, "
                f"but got {type(old_doc).__name__}"
            )

        return old_doc, cas

    def _update_runtime_metrics(self, stats: BatchStats, started_perf: float) -> None:
        duration_seconds = time.perf_counter() - started_perf
        stats.duration_seconds = round(duration_seconds, 3)
        stats.duration_ms = int(duration_seconds * 1000)

        stats.completed_count = stats.processed_count + stats.failed_count
        stats.in_flight_count = max(stats.received_count - stats.completed_count, 0)

        if duration_seconds > 0:
            stats.records_per_second = round(stats.received_count / duration_seconds, 2)
            stats.scanned_records_per_second = round(
                stats.scanned_count / duration_seconds,
                2,
            )
        else:
            stats.records_per_second = None
            stats.scanned_records_per_second = None

    async def _print_progress(
        self,
        stats: BatchStats,
        started_perf: float,
        *,
        final: bool = False,
    ) -> None:
        self._update_runtime_metrics(stats, started_perf)

        label = "final" if final else "progress"

        print(
            (
                f"[{label}] "
                f"worker={stats.worker_id}/{stats.worker_count} "
                f"scanned={stats.scanned_count:,} "
                f"assigned={stats.received_count:,} "
                f"completed={stats.completed_count:,} "
                f"in_flight={stats.in_flight_count:,} "
                f"inserted={stats.inserted_count:,} "
                f"changed={stats.changed_count:,} "
                f"unchanged={stats.unchanged_count:,} "
                f"failed={stats.failed_count:,} "
                f"hash_match={stats.hash_match_count:,} "
                f"hash_changed={stats.hash_changed_count:,} "
                f"hash_miss={stats.hash_miss_count:,} "
                f"hash_upsert={stats.hash_upsert_count:,} "
                f"assigned_rps={stats.records_per_second} "
                f"scan_rps={stats.scanned_records_per_second}"
            ),
            flush=True,
        )

    async def _write_batch_control(self, stats: BatchStats) -> None:
        if self.dry_run or self.repository is None:
            return

        worker_doc = {
            **asdict(stats),
        }

        await self.repository.upsert_batch_worker_stats(
            batch_id=stats.batch_id,
            worker_id=stats.worker_id,
            worker_count=stats.worker_count,
            worker_doc=worker_doc,
        )

    async def _write_deadletter(
        self,
        *,
        batch_id: str,
        source_file: str,
        row_number: int,
        row: dict[str, Any],
        error: str,
        traceback_text: str,
    ) -> None:
        doc = {
            "type": "dead_letter",
            "batch_id": batch_id,
            "source_file": source_file,
            "row_number": row_number,
            "worker_id": self.worker_id,
            "worker_count": self.worker_count,
            "error": error,
            "traceback": traceback_text,
            "raw_record": row,
            "created_at": utc_now_iso(),
        }

        if self.dry_run or self.repository is None:
            print("\n--- DRY RUN DEAD LETTER ---")
            print_json(doc)
            return

        await self.repository.insert_deadletter(
            f"deadletter::{batch_id}::worker-{self.worker_id}::{row_number}",
            doc,
        )


def print_json(value: Any) -> None:
    try:
        import orjson

        print(
            orjson.dumps(
                value,
                option=orjson.OPT_INDENT_2 | orjson.OPT_SORT_KEYS,
            ).decode("utf-8")
        )
    except Exception:
        print(json.dumps(value, indent=2, sort_keys=True, default=str))