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


# Couchbase Python SDK versions differ slightly in exception names.
# Your SDK uses TemporaryFailException, not TemporaryFailureException.
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
    Async Couchbase repository for high-throughput fare ingestion.

    Important behavior:
    - Uses async KV operations.
    - Retries transient KV errors.
    - Handles ambiguous insert timeouts by treating DocumentExistsException
      during retry as a non-fatal idempotency outcome.
    """

    def __init__(self, config: CouchbaseConfig):
        self.config = config
        self.cluster: Cluster | None = None

        self.current = None
        self.batch = None
        self.deadletter = None
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

                # Small jitter prevents many concurrent tasks from retrying together.
                jitter = min(0.025 * attempt, 0.1)
                await asyncio.sleep(backoff + jitter)

            except CouchbaseException:
                # Non-transient SDK exception. Let caller decide.
                raise

        if last_exc is not None:
            raise last_exc

        raise RuntimeError(f"{operation_name} failed unexpectedly")

    @staticmethod
    def content_as_dict(result: Any) -> dict[str, Any]:
        """
        Decode Couchbase result content into a dict.

        Normal case:
            result.content_as[dict]

        Recovery case:
            an earlier version accidentally stored JSON as a string.
        """
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

    async def insert_current(self, key: str, doc: dict[str, Any]) -> bool:
        async def op():
            return await self.current.insert(key, doc)

        try:
            await self._retry("insert_current", op)
            return True

        except DocumentExistsException:
            # Important for ambiguous timeout recovery.
            # The first insert may have succeeded, then retry sees it exists.
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

    async def insert_history(self, key: str, doc: dict[str, Any]) -> bool:
        async def op():
            return await self.history.insert(key, doc)

        try:
            await self._retry("insert_history", op)
            return True

        except DocumentExistsException:
            # Idempotent retry of same history event.
            return False

    async def upsert_batch(self, key: str, doc: dict[str, Any]) -> None:
        async def op():
            return await self.batch.upsert(key, doc)

        await self._retry("upsert_batch", op)

    async def insert_deadletter(self, key: str, doc: dict[str, Any]) -> None:
        async def op():
            return await self.deadletter.upsert(key, doc)

        await self._retry("insert_deadletter", op)

    async def close(self) -> None:
        if self.cluster is None:
            return

        try:
            await self.cluster.close()
        except TypeError:
            self.cluster.close()
        except AttributeError:
            pass