from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable

from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    AmbiguousTimeoutException,
    CasMismatchException,
    CouchbaseException,
    DocumentExistsException,
    DocumentNotFoundException,
    RequestCanceledException,
    UnAmbiguousTimeoutException,
)
from couchbase.options import ClusterOptions, ReplaceOptions

from .config import CouchbaseConfig


try:
    from couchbase.exceptions import TemporaryFailException
except ImportError:  # pragma: no cover
    TemporaryFailException = None  # type: ignore


def _build_transient_exceptions() -> tuple[type[BaseException], ...]:
    exceptions: list[type[BaseException]] = [
        AmbiguousTimeoutException,
        UnAmbiguousTimeoutException,
        RequestCanceledException,
    ]

    if TemporaryFailException is not None:
        exceptions.append(TemporaryFailException)

    return tuple(exceptions)


TRANSIENT_EXCEPTIONS = _build_transient_exceptions()


class AsyncCouchbaseFareRepository:
    """
    Async Couchbase repository.

    Supports:
    - retry/backoff
    - async KV operations
    - hash-only collection
    - shared batch_control document
    - chunked/bulk-style pipeline from processor
    """

    def __init__(self, config: CouchbaseConfig):
        self.config = config
        self.cluster: Cluster | None = None

        self.current = None
        self.batch = None
        self.deadletter = None
        self.hashes = None
        self.history = None

        self.max_retries = 5
        self.base_backoff_seconds = 0.05
        self.max_backoff_seconds = 1.0

    async def connect(self) -> None:
        print("[couchbase] creating authenticator", flush=True)
        auth = PasswordAuthenticator(self.config.username, self.config.password)

        print(f"[couchbase] connecting to {self.config.connstr}", flush=True)
        self.cluster = Cluster(
            self.config.connstr,
            ClusterOptions(auth),
        )

        print(f"[couchbase] opening bucket {self.config.bucket}", flush=True)
        fares_bucket = self.cluster.bucket(self.config.bucket)
        await fares_bucket.on_connect()

        print(f"[couchbase] opening scope {self.config.scope}", flush=True)
        fares_scope = fares_bucket.scope(self.config.scope)

        print(f"[couchbase] opening collection {self.config.current_collection}", flush=True)
        self.current = fares_scope.collection(self.config.current_collection)

        print(f"[couchbase] opening collection {self.config.batch_collection}", flush=True)
        self.batch = fares_scope.collection(self.config.batch_collection)

        print(f"[couchbase] opening collection {self.config.deadletter_collection}", flush=True)
        self.deadletter = fares_scope.collection(self.config.deadletter_collection)

        print(f"[couchbase] opening collection {self.config.hash_collection}", flush=True)
        self.hashes = fares_scope.collection(self.config.hash_collection)

        print(f"[couchbase] opening history bucket {self.config.history_bucket}", flush=True)
        history_bucket = self.cluster.bucket(self.config.history_bucket)
        await history_bucket.on_connect()

        print(f"[couchbase] opening history scope {self.config.history_scope}", flush=True)
        history_scope = history_bucket.scope(self.config.history_scope)

        print(f"[couchbase] opening history collection {self.config.history_collection}", flush=True)
        self.history = history_scope.collection(self.config.history_collection)

        await self._test_collections()

        print("[couchbase] async repository initialized", flush=True)

    async def _test_collections(self) -> None:
        tests = [
            ("current_fares", self.current),
            ("batch_control", self.batch),
            ("dead_letter", self.deadletter),
            ("fare_hashes", self.hashes),
            ("fare_changes_7d", self.history),
        ]

        for name, collection in tests:
            print(f"[couchbase] testing collection access: {name}", flush=True)
            try:
                await collection.get("__connection_test__")
            except DocumentNotFoundException:
                print(f"[couchbase] collection reachable: {name}", flush=True)

    async def _retry(
        self,
        operation_name: str,
        fn: Callable[[], Awaitable[Any]],
    ) -> Any:
        last_exc: Exception | None = None

        for attempt in range(self.max_retries + 1):
            try:
                return await fn()

            except TRANSIENT_EXCEPTIONS as exc:
                last_exc = exc

                if attempt >= self.max_retries:
                    raise

                backoff = min(
                    self.base_backoff_seconds * (2 ** attempt),
                    self.max_backoff_seconds,
                )
                jitter = min(0.025 * attempt, 0.1)
                await asyncio.sleep(backoff + jitter)

            except CouchbaseException:
                raise

        if last_exc is not None:
            raise last_exc

        raise RuntimeError(f"{operation_name} failed unexpectedly")

    @staticmethod
    def content_as_dict(result: Any) -> dict[str, Any]:
        try:
            content = result.content_as[dict]
        except Exception:
            try:
                content = result.content_as[str]
            except Exception:
                content = result.content_as(dict)

        if isinstance(content, dict):
            return content

        if isinstance(content, str):
            decoded = json.loads(content)
            if isinstance(decoded, dict):
                return decoded

        raise TypeError(
            f"Expected document content to be dict or JSON string, "
            f"but got {type(content).__name__}"
        )

    async def get_current(self, key: str) -> tuple[dict[str, Any], Any] | None:
        async def op():
            return await self.current.get(key)

        try:
            result = await self._retry("get_current", op)
            return self.content_as_dict(result), result.cas
        except DocumentNotFoundException:
            return None

    async def get_hash(self, key: str) -> dict[str, Any] | None:
        async def op():
            return await self.hashes.get(key)

        try:
            result = await self._retry("get_hash", op)
            return self.content_as_dict(result)
        except DocumentNotFoundException:
            return None

    async def insert_current(self, key: str, doc: dict[str, Any]) -> bool:
        async def op():
            return await self.current.insert(key, doc)

        try:
            await self._retry("insert_current", op)
            return True
        except DocumentExistsException:
            return False

    async def upsert_current(self, key: str, doc: dict[str, Any]) -> bool:
        async def op():
            return await self.current.upsert(key, doc)

        await self._retry("upsert_current", op)
        return True

    async def replace_current(self, key: str, doc: dict[str, Any], cas: Any) -> bool:
        async def op():
            return await self.current.replace(key, doc, ReplaceOptions(cas=cas))

        try:
            await self._retry("replace_current", op)
            return True
        except CasMismatchException:
            return False

    async def upsert_hash(self, key: str, doc: dict[str, Any]) -> None:
        async def op():
            return await self.hashes.upsert(key, doc)

        await self._retry("upsert_hash", op)

    async def insert_history(self, key: str, doc: dict[str, Any]) -> bool:
        async def op():
            return await self.history.insert(key, doc)

        try:
            await self._retry("insert_history", op)
            return True
        except DocumentExistsException:
            return False

    async def insert_deadletter(self, key: str, doc: dict[str, Any]) -> None:
        async def op():
            return await self.deadletter.upsert(key, doc)

        await self._retry("insert_deadletter", op)

    async def upsert_batch(self, key: str, doc: dict[str, Any]) -> None:
        async def op():
            return await self.batch.upsert(key, doc)

        await self._retry("upsert_batch", op)

    async def upsert_batch_worker_stats(
        self,
        batch_id: str,
        worker_id: int,
        worker_count: int,
        worker_doc: dict[str, Any],
    ) -> None:
        worker_key = str(worker_id)

        for attempt in range(self.max_retries + 1):
            try:
                result = await self.batch.get(batch_id)
                batch_doc = self.content_as_dict(result)
                cas = result.cas

            except DocumentNotFoundException:
                batch_doc = {
                    "type": "fare_batch",
                    "batch_id": batch_id,
                    "worker_count": worker_count,
                    "status": "PROCESSING",
                    "workers": {},
                    "totals": {},
                }

                batch_doc["workers"][worker_key] = worker_doc
                batch_doc["worker_count"] = worker_count
                self._recalculate_batch_totals(batch_doc)

                try:
                    await self.batch.insert(batch_id, batch_doc)
                    return
                except DocumentExistsException:
                    await asyncio.sleep(0.05 * (attempt + 1))
                    continue

            batch_doc.setdefault("workers", {})
            batch_doc["workers"][worker_key] = worker_doc
            batch_doc["worker_count"] = worker_count
            self._recalculate_batch_totals(batch_doc)

            try:
                await self.batch.replace(batch_id, batch_doc, ReplaceOptions(cas=cas))
                return
            except CasMismatchException:
                await asyncio.sleep(0.05 * (attempt + 1))
                continue

        raise RuntimeError(f"Failed to update shared batch_control document {batch_id}")

    @staticmethod
    def _recalculate_batch_totals(batch_doc: dict[str, Any]) -> None:
        workers = batch_doc.get("workers", {})
        worker_values = list(workers.values())

        numeric_fields = [
            "scanned_count",
            "skipped_by_worker_count",
            "received_count",
            "processed_count",
            "inserted_count",
            "changed_count",
            "unchanged_count",
            "failed_count",
            "cas_retry_count",
            "history_duplicate_count",
            "skipped_new_history_count",
            "hash_match_count",
            "hash_miss_count",
            "hash_changed_count",
            "hash_upsert_count",
            "completed_count",
            "in_flight_count",
            "hash_backfill_count",
        ]

        totals: dict[str, Any] = {}

        for field in numeric_fields:
            totals[field] = sum(
                int(worker.get(field, 0) or 0)
                for worker in worker_values
            )

        durations = [
            float(worker.get("duration_seconds", 0) or 0)
            for worker in worker_values
        ]

        wall_clock_seconds = max(durations) if durations else 0

        totals["wall_clock_seconds_estimate"] = round(wall_clock_seconds, 3)
        totals["worker_count_reported"] = len(worker_values)

        if wall_clock_seconds > 0:
            totals["aggregate_records_per_second"] = round(
                totals["received_count"] / wall_clock_seconds,
                2,
            )
            totals["aggregate_scanned_records_per_second"] = round(
                totals["scanned_count"] / wall_clock_seconds,
                2,
            )
        else:
            totals["aggregate_records_per_second"] = None
            totals["aggregate_scanned_records_per_second"] = None

        batch_doc["totals"] = totals

        expected_workers = int(batch_doc.get("worker_count", 1) or 1)
        completed_workers = [
            worker
            for worker in worker_values
            if worker.get("status") in {"COMPLETED", "COMPLETED_WITH_ERRORS"}
        ]

        if len(completed_workers) < expected_workers:
            batch_doc["status"] = "PROCESSING"
            batch_doc["completed_at"] = None
        else:
            batch_doc["status"] = (
                "COMPLETED_WITH_ERRORS"
                if totals["failed_count"] > 0
                else "COMPLETED"
            )

            completed_times = [
                worker.get("completed_at")
                for worker in worker_values
                if worker.get("completed_at")
            ]
            batch_doc["completed_at"] = max(completed_times) if completed_times else None

        started_times = [
            worker.get("started_at")
            for worker in worker_values
            if worker.get("started_at")
        ]
        batch_doc["started_at"] = min(started_times) if started_times else None

    async def close(self) -> None:
        if self.cluster is None:
            return

        try:
            await self.cluster.close()
        except TypeError:
            self.cluster.close()
        except AttributeError:
            pass