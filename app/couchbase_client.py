from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from acouchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    CasMismatchException,
    DocumentExistsException,
    DocumentNotFoundException,
)
from couchbase.options import ClusterOptions, ReplaceOptions

from .config import CouchbaseConfig


class AsyncCouchbaseFareRepository:
    """
    Async Couchbase repository.

    This replaces the synchronous one-row-at-a-time repository and allows
    the processor to keep many KV operations in flight at the same time.
    """

    def __init__(self, config: CouchbaseConfig):
        self.config = config
        self.cluster: Cluster | None = None

        self.current = None
        self.batch = None
        self.deadletter = None
        self.history = None

    async def connect(self) -> None:
        print("[couchbase] creating authenticator", flush=True)
        auth = PasswordAuthenticator(self.config.username, self.config.password)

        print(f"[couchbase] connecting to {self.config.connstr}", flush=True)

        self.cluster = Cluster(
            self.config.connstr,
            ClusterOptions(auth),
        )

        fares_bucket = self.cluster.bucket(self.config.bucket)
        await fares_bucket.on_connect()

        fares_scope = fares_bucket.scope(self.config.scope)

        self.current = fares_scope.collection(self.config.current_collection)
        self.batch = fares_scope.collection(self.config.batch_collection)
        self.deadletter = fares_scope.collection(self.config.deadletter_collection)

        history_bucket = self.cluster.bucket(self.config.history_bucket)
        await history_bucket.on_connect()

        history_scope = history_bucket.scope(self.config.history_scope)
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
        try:
            result = await self.current.get(key)
            return self.content_as_dict(result), result.cas
        except DocumentNotFoundException:
            return None

    async def insert_current(self, key: str, doc: dict[str, Any]) -> bool:
        try:
            await self.current.insert(key, doc)
            return True
        except DocumentExistsException:
            return False

    async def replace_current(self, key: str, doc: dict[str, Any], cas: Any) -> bool:
        try:
            await self.current.replace(key, doc, ReplaceOptions(cas=cas))
            return True
        except CasMismatchException:
            return False

    async def insert_history(self, key: str, doc: dict[str, Any]) -> bool:
        try:
            await self.history.insert(key, doc)
            return True
        except DocumentExistsException:
            return False

    async def upsert_batch(self, key: str, doc: dict[str, Any]) -> None:
        await self.batch.upsert(key, doc)

    async def insert_deadletter(self, key: str, doc: dict[str, Any]) -> None:
        await self.deadletter.upsert(key, doc)

    async def close(self) -> None:
        if self.cluster is None:
            return

        try:
            await self.cluster.close()
        except TypeError:
            self.cluster.close()
        except AttributeError:
            pass