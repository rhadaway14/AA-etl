from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
import json
import time
import traceback

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
    received_count: int = 0
    processed_count: int = 0
    inserted_count: int = 0
    changed_count: int = 0
    unchanged_count: int = 0
    failed_count: int = 0
    cas_retry_count: int = 0
    history_duplicate_count: int = 0
    started_at: str = ""
    completed_at: str | None = None

    # Runtime metrics
    duration_seconds: float | None = None
    duration_ms: int | None = None
    records_per_second: float | None = None


class FareBatchProcessor:
    def __init__(
        self,
        repository: Any | None,
        dry_run: bool = False,
        preview: int = 0,
        max_cas_retries: int = 3,
    ):
        self.repository = repository
        self.dry_run = dry_run
        self.preview = preview
        self.max_cas_retries = max_cas_retries

    def process_csv(self, csv_path: str, batch_id: str) -> BatchStats:
        source_file = Path(csv_path).name
        started_perf = time.perf_counter()

        stats = BatchStats(
            batch_id=batch_id,
            source_file=source_file,
            started_at=utc_now_iso(),
        )

        self._write_batch_control(stats)

        for row_number, row in iter_csv_rows(csv_path):
            stats.received_count += 1

            try:
                self._process_row(
                    row=row,
                    row_number=row_number,
                    source_file=source_file,
                    batch_id=batch_id,
                    stats=stats,
                )
                stats.processed_count += 1

            except Exception as exc:
                stats.failed_count += 1

                print("")
                print("=" * 80, flush=True)
                print(f"[error] Failed processing row {row_number}", flush=True)
                print(f"[error] {type(exc).__name__}: {exc}", flush=True)
                print(traceback.format_exc(limit=8), flush=True)
                print("=" * 80, flush=True)
                print("")

                self._write_deadletter(
                    batch_id=batch_id,
                    source_file=source_file,
                    row_number=row_number,
                    row=row,
                    error=str(exc),
                    traceback_text=traceback.format_exc(limit=8),
                )

            # Cheap checkpointing. For 10M rows, you may want this at 10k or 100k.
            if stats.received_count % 1000 == 0:
                self._update_runtime_metrics(stats, started_perf)
                self._write_batch_control(stats)

        stats.status = "COMPLETED" if stats.failed_count == 0 else "COMPLETED_WITH_ERRORS"
        stats.completed_at = utc_now_iso()
        self._update_runtime_metrics(stats, started_perf)

        self._write_batch_control(stats)

        return stats

    def _update_runtime_metrics(self, stats: BatchStats, started_perf: float) -> None:
        duration_seconds = time.perf_counter() - started_perf
        stats.duration_seconds = round(duration_seconds, 3)
        stats.duration_ms = int(duration_seconds * 1000)

        if duration_seconds > 0:
            stats.records_per_second = round(stats.received_count / duration_seconds, 2)
        else:
            stats.records_per_second = None

    def _coerce_existing_doc(
        self,
        current_result: Any,
    ) -> tuple[dict[str, Any], Any]:
        """
        Normalize whatever repository.get_current() returns.

        Preferred repository return shape:
            (document_dict, cas)

        Also supports:
            {"content": document_dict, "cas": cas}

        And recovery case:
            document content stored as JSON string.
        """
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

    def _process_row(
        self,
        *,
        row: dict[str, Any],
        row_number: int,
        source_file: str,
        batch_id: str,
        stats: BatchStats,
    ) -> None:
        now = utc_now_iso()

        parts = row_to_parts(
            row=row,
            batch_id=batch_id,
            source_file=source_file,
            row_number=row_number,
        )

        new_doc = build_current_document(parts=parts, now=now)

        if self.dry_run:
            stats.inserted_count += 1

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

                print("\n--- DRY RUN HISTORY DOC ---")
                print_json(history_doc)

            return

        if self.repository is None:
            raise RuntimeError("Repository is required when dry_run=False")

        for _attempt in range(self.max_cas_retries + 1):
            existing = self.repository.get_current(parts.fare_key)

            if existing is None:
                history_doc = build_history_document(
                    parts=parts,
                    change_type="NEW_FARE",
                    old_doc=None,
                    new_doc=new_doc,
                    batch_id=batch_id,
                    now=now,
                )

                inserted_history = self.repository.insert_history(
                    history_key(parts, batch_id),
                    history_doc,
                )

                if not inserted_history:
                    stats.history_duplicate_count += 1

                inserted_current = self.repository.insert_current(
                    parts.fare_key,
                    new_doc,
                )

                if inserted_current:
                    stats.inserted_count += 1
                    return

                # Someone else inserted it first. Re-read and process as update/unchanged.
                stats.cas_retry_count += 1
                continue

            old_doc, cas = self._coerce_existing_doc(existing)
            old_hash = old_doc.get("business_hash")

            if old_hash == parts.business_hash:
                stats.unchanged_count += 1
                return

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

            inserted_history = self.repository.insert_history(
                history_key(parts, batch_id),
                history_doc,
            )

            if not inserted_history:
                stats.history_duplicate_count += 1

            replaced = self.repository.replace_current(
                parts.fare_key,
                updated_doc,
                cas=cas,
            )

            if replaced:
                stats.changed_count += 1
                return

            stats.cas_retry_count += 1

        raise RuntimeError(f"CAS retries exceeded for {parts.fare_key}")

    def _write_batch_control(self, stats: BatchStats) -> None:
        if self.dry_run or self.repository is None:
            return

        doc = {
            "type": "fare_batch",
            **asdict(stats),
        }

        self.repository.upsert_batch(stats.batch_id, doc)

    def _write_deadletter(
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
            "error": error,
            "traceback": traceback_text,
            "raw_record": row,
            "created_at": utc_now_iso(),
        }

        if self.dry_run or self.repository is None:
            print("\n--- DRY RUN DEAD LETTER ---")
            print_json(doc)
            return

        self.repository.insert_deadletter(
            f"deadletter::{batch_id}::{row_number}",
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
        import json

        print(json.dumps(value, indent=2, sort_keys=True, default=str))