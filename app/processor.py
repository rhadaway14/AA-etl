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
    input_partition: bool = False

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
    hash_backfill_count: int = 0

    # Lazy document construction metric.
    # For a 10M file with 100k changes, this should be ~100k, not 10M.
    current_doc_build_count: int = 0

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
        chunk_size: int = 1000,
        chunk_concurrency: int = 2,
        progress_every: int = 10000,
        batch_control_every: int = 100000,
        max_cas_retries: int = 3,
        skip_new_history: bool = False,
        initial_load: bool = False,
        hash_backfill: bool = False,
        input_partition: bool = False,
        worker_count: int = 1,
        worker_id: int = 0,
    ):
        self.repository = repository
        self.dry_run = dry_run
        self.preview = preview
        self.chunk_size = chunk_size
        self.chunk_concurrency = chunk_concurrency
        self.progress_every = progress_every
        self.batch_control_every = batch_control_every
        self.max_cas_retries = max_cas_retries
        self.skip_new_history = skip_new_history
        self.initial_load = initial_load
        self.hash_backfill = hash_backfill
        self.input_partition = input_partition
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
            input_partition=self.input_partition,
            started_at=utc_now_iso(),
        )

        await self._write_batch_control(stats)

        pending_chunks: set[asyncio.Task] = set()
        chunk: list[tuple[int, dict[str, Any]]] = []

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

            chunk.append((row_number, row))

            if len(chunk) >= self.chunk_size:
                task = asyncio.create_task(
                    self._process_chunk_safely(
                        chunk=chunk,
                        source_file=source_file,
                        batch_id=batch_id,
                        stats=stats,
                    )
                )
                pending_chunks.add(task)
                chunk = []

            if len(pending_chunks) >= self.chunk_concurrency:
                done, pending_chunks = await asyncio.wait(
                    pending_chunks,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for done_task in done:
                    await done_task

            if scanned % self.progress_every == 0:
                await self._print_progress(stats, started_perf)

            if scanned % self.batch_control_every == 0:
                self._update_runtime_metrics(stats, started_perf)
                await self._write_batch_control(stats)

        if chunk:
            pending_chunks.add(
                asyncio.create_task(
                    self._process_chunk_safely(
                        chunk=chunk,
                        source_file=source_file,
                        batch_id=batch_id,
                        stats=stats,
                    )
                )
            )

        if pending_chunks:
            done, _ = await asyncio.wait(pending_chunks)
            for done_task in done:
                await done_task

        stats.status = "COMPLETED" if stats.failed_count == 0 else "COMPLETED_WITH_ERRORS"
        stats.completed_at = utc_now_iso()
        self._update_runtime_metrics(stats, started_perf)

        await self._write_batch_control(stats)
        await self._print_progress(stats, started_perf, final=True)

        return stats

    def _row_belongs_to_worker(self, row: dict[str, Any]) -> bool:
        if self.input_partition:
            return True

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

    @staticmethod
    def _build_tiny_hash_doc(business_hash: Any) -> dict[str, Any]:
        """
        Compact fare_hashes document.

        Old format:
            {
              "type": "fare_hash",
              "fare_key": "...",
              "source_key": "...",
              "business_hash": "...",
              "last_batch_id": "...",
              "updated_at": "..."
            }

        New format:
            {
              "h": "..."
            }

        The comparison hot path reads 10M of these docs, so smaller is better.
        """
        return {
            "h": business_hash,
        }

    @staticmethod
    def _hash_value_from_doc(hash_doc: dict[str, Any] | None) -> Any:
        """
        Read both the new tiny hash format and the old verbose format.

        New:
            {"h": "..."}

        Old:
            {"business_hash": "..."}
        """
        if not hash_doc:
            return None

        if "h" in hash_doc:
            return hash_doc.get("h")

        return hash_doc.get("business_hash")

    async def _process_chunk_safely(
        self,
        *,
        chunk: list[tuple[int, dict[str, Any]]],
        source_file: str,
        batch_id: str,
        stats: BatchStats,
    ) -> None:
        try:
            await self._process_chunk(
                chunk=chunk,
                source_file=source_file,
                batch_id=batch_id,
                stats=stats,
            )

        except Exception as exc:
            async with self._stats_lock:
                stats.failed_count += len(chunk)

            for row_number, row in chunk:
                await self._write_deadletter(
                    batch_id=batch_id,
                    source_file=source_file,
                    row_number=row_number,
                    row=row,
                    error=str(exc),
                    traceback_text=traceback.format_exc(limit=8),
                )

    async def _process_chunk(
        self,
        *,
        chunk: list[tuple[int, dict[str, Any]]],
        source_file: str,
        batch_id: str,
        stats: BatchStats,
    ) -> None:
        now = utc_now_iso()
        items: list[dict[str, Any]] = []

        for row_number, row in chunk:
            try:
                parts = row_to_parts(
                    row=row,
                    batch_id=batch_id,
                    source_file=source_file,
                    row_number=row_number,
                )

                hash_key = self._hash_key_for_fare_key(parts.fare_key)
                hash_doc = self._build_tiny_hash_doc(parts.business_hash)

                item: dict[str, Any] = {
                    "row_number": row_number,
                    "row": row,
                    "parts": parts,
                    "hash_key": hash_key,
                    "hash_doc": hash_doc,
                }

                # Initial load still needs to build full docs for every row.
                # Normal comparison mode does NOT build full docs here.
                if self.initial_load or self.dry_run:
                    item["current_doc"] = build_current_document(parts=parts, now=now)

                items.append(item)

            except Exception as exc:
                async with self._stats_lock:
                    stats.failed_count += 1

                await self._write_deadletter(
                    batch_id=batch_id,
                    source_file=source_file,
                    row_number=row_number,
                    row=row,
                    error=str(exc),
                    traceback_text=traceback.format_exc(limit=8),
                )

        if not items:
            return

        if self.dry_run:
            await self._apply_counts(
                stats,
                processed=len(items),
                inserted=len(items),
                current_doc_build=len(items),
            )
            return

        if self.hash_backfill:
            await self._bulk_hash_backfill(items, stats)
            return

        if self.initial_load:
            await self._bulk_initial_load(items, stats)
            return

        await self._bulk_compare_and_apply(
            items=items,
            stats=stats,
            batch_id=batch_id,
            now=now,
        )

    async def _bulk_hash_backfill(
        self,
        items: list[dict[str, Any]],
        stats: BatchStats,
    ) -> None:
        results = await asyncio.gather(
            *[
                self.repository.upsert_hash(item["hash_key"], item["hash_doc"])
                for item in items
            ],
            return_exceptions=True,
        )

        processed = 0
        failed = 0

        for item, result in zip(items, results):
            if isinstance(result, Exception):
                failed += 1
                await self._write_deadletter(
                    batch_id=stats.batch_id,
                    source_file=stats.source_file,
                    row_number=item["row_number"],
                    row=item["row"],
                    error=str(result),
                    traceback_text="bulk hash backfill failure",
                )
            else:
                processed += 1

        await self._apply_counts(
            stats,
            processed=processed,
            failed=failed,
            hash_backfill=processed,
            hash_upsert=processed,
        )

    async def _bulk_initial_load(
        self,
        items: list[dict[str, Any]],
        stats: BatchStats,
    ) -> None:
        current_results = await asyncio.gather(
            *[
                self.repository.upsert_current(
                    item["parts"].fare_key,
                    item["current_doc"],
                )
                for item in items
            ],
            return_exceptions=True,
        )

        hash_results = await asyncio.gather(
            *[
                self.repository.upsert_hash(item["hash_key"], item["hash_doc"])
                for item in items
            ],
            return_exceptions=True,
        )

        processed = 0
        failed = 0

        for item, current_result, hash_result in zip(items, current_results, hash_results):
            if isinstance(current_result, Exception) or isinstance(hash_result, Exception):
                failed += 1
                error = current_result if isinstance(current_result, Exception) else hash_result
                await self._write_deadletter(
                    batch_id=stats.batch_id,
                    source_file=stats.source_file,
                    row_number=item["row_number"],
                    row=item["row"],
                    error=str(error),
                    traceback_text="bulk initial-load failure",
                )
            else:
                processed += 1

        await self._apply_counts(
            stats,
            processed=processed,
            failed=failed,
            inserted=processed,
            skipped_new_history=processed,
            hash_upsert=processed,
            current_doc_build=len(items),
        )

    async def _bulk_compare_and_apply(
        self,
        *,
        items: list[dict[str, Any]],
        stats: BatchStats,
        batch_id: str,
        now: str,
    ) -> None:
        # Stage 1: read only tiny hash docs.
        # For unchanged rows, this is the only Couchbase read.
        hash_results = await asyncio.gather(
            *[self.repository.get_hash(item["hash_key"]) for item in items],
            return_exceptions=True,
        )

        hash_matches: list[dict[str, Any]] = []
        candidates: list[dict[str, Any]] = []
        hash_failures: list[tuple[dict[str, Any], Exception]] = []

        for item, result in zip(items, hash_results):
            if isinstance(result, Exception):
                hash_failures.append((item, result))
                continue

            if result is None:
                item["hash_state"] = "miss"
                candidates.append(item)
                continue

            existing_hash = self._hash_value_from_doc(result)

            if existing_hash == item["parts"].business_hash:
                hash_matches.append(item)
            else:
                item["hash_state"] = "changed"
                candidates.append(item)

        for item, exc in hash_failures:
            await self._write_deadletter(
                batch_id=batch_id,
                source_file=stats.source_file,
                row_number=item["row_number"],
                row=item["row"],
                error=str(exc),
                traceback_text="bulk hash get failure",
            )

        processed = len(hash_matches)
        failed = len(hash_failures)
        unchanged = len(hash_matches)
        hash_match_count = len(hash_matches)
        hash_miss_count = sum(1 for item in candidates if item.get("hash_state") == "miss")
        hash_changed_count = sum(1 for item in candidates if item.get("hash_state") == "changed")

        if not candidates:
            await self._apply_counts(
                stats,
                processed=processed,
                failed=failed,
                unchanged=unchanged,
                hash_match=hash_match_count,
            )
            return

        # Stage 2: lazy full document construction.
        # Only hash misses/changed rows reach this point.
        for item in candidates:
            item["current_doc"] = build_current_document(
                parts=item["parts"],
                now=now,
            )

        current_doc_build_count = len(candidates)

        current_results = await asyncio.gather(
            *[self.repository.get_current(item["parts"].fare_key) for item in candidates],
            return_exceptions=True,
        )

        apply_tasks = []

        for item, current_result in zip(candidates, current_results):
            if isinstance(current_result, Exception):
                failed += 1
                apply_tasks.append(
                    self._write_deadletter(
                        batch_id=batch_id,
                        source_file=stats.source_file,
                        row_number=item["row_number"],
                        row=item["row"],
                        error=str(current_result),
                        traceback_text="bulk current get failure",
                    )
                )
                continue

            apply_tasks.append(
                self._apply_candidate(
                    item=item,
                    current_result=current_result,
                    batch_id=batch_id,
                    now=now,
                    stats=stats,
                )
            )

        apply_results = await asyncio.gather(
            *apply_tasks,
            return_exceptions=True,
        )

        inserted = 0
        changed = 0
        candidate_unchanged = 0
        hash_upsert = 0

        for item, result in zip(candidates, apply_results):
            if isinstance(result, Exception):
                failed += 1
                await self._write_deadletter(
                    batch_id=batch_id,
                    source_file=stats.source_file,
                    row_number=item["row_number"],
                    row=item["row"],
                    error=str(result),
                    traceback_text="bulk candidate apply failure",
                )
                continue

            processed += 1

            if result == "inserted":
                inserted += 1
                hash_upsert += 1
            elif result == "changed":
                changed += 1
                hash_upsert += 1
            elif result == "unchanged":
                candidate_unchanged += 1
                hash_upsert += 1

        await self._apply_counts(
            stats,
            processed=processed,
            failed=failed,
            inserted=inserted,
            changed=changed,
            unchanged=unchanged + candidate_unchanged,
            hash_match=hash_match_count,
            hash_miss=hash_miss_count,
            hash_changed=hash_changed_count,
            hash_upsert=hash_upsert,
            current_doc_build=current_doc_build_count,
        )

    async def _apply_candidate(
        self,
        *,
        item: dict[str, Any],
        current_result: Any,
        batch_id: str,
        now: str,
        stats: BatchStats,
    ) -> str:
        parts = item["parts"]
        current_doc = item["current_doc"]
        hash_key = item["hash_key"]
        hash_doc = item["hash_doc"]

        if current_result is None:
            if not self.skip_new_history:
                history_doc = build_history_document(
                    parts=parts,
                    change_type="NEW_FARE",
                    old_doc=None,
                    new_doc=current_doc,
                    batch_id=batch_id,
                    now=now,
                )

                await self.repository.insert_history(
                    history_key(parts, batch_id),
                    history_doc,
                )

            inserted = await self.repository.insert_current(
                parts.fare_key,
                current_doc,
            )

            await self.repository.upsert_hash(hash_key, hash_doc)

            if inserted:
                return "inserted"

            return "unchanged"

        old_doc, cas = self._coerce_existing_doc(current_result)
        old_hash = old_doc.get("business_hash")

        if old_hash == parts.business_hash:
            await self.repository.upsert_hash(hash_key, hash_doc)
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

        if not replaced:
            raise RuntimeError(f"CAS mismatch for {parts.fare_key}")

        await self.repository.upsert_hash(hash_key, hash_doc)
        return "changed"

    async def _apply_counts(
        self,
        stats: BatchStats,
        *,
        processed: int = 0,
        failed: int = 0,
        inserted: int = 0,
        changed: int = 0,
        unchanged: int = 0,
        skipped_new_history: int = 0,
        hash_match: int = 0,
        hash_miss: int = 0,
        hash_changed: int = 0,
        hash_upsert: int = 0,
        hash_backfill: int = 0,
        current_doc_build: int = 0,
    ) -> None:
        async with self._stats_lock:
            stats.processed_count += processed
            stats.failed_count += failed
            stats.inserted_count += inserted
            stats.changed_count += changed
            stats.unchanged_count += unchanged
            stats.skipped_new_history_count += skipped_new_history
            stats.hash_match_count += hash_match
            stats.hash_miss_count += hash_miss
            stats.hash_changed_count += hash_changed
            stats.hash_upsert_count += hash_upsert
            stats.hash_backfill_count += hash_backfill
            stats.current_doc_build_count += current_doc_build

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
                f"input_partition={stats.input_partition} "
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
                f"hash_backfill={stats.hash_backfill_count:,} "
                f"current_doc_build={stats.current_doc_build_count:,} "
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
            "input_partition": self.input_partition,
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