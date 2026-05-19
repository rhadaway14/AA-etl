from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import (
    CasMismatchException,
    DocumentExistsException,
    DocumentNotFoundException,
)
from couchbase.options import ClusterOptions, ReplaceOptions

from .config import CouchbaseConfig


class CouchbaseFareRepository:
    def __init__(self, config: CouchbaseConfig):
        self.config = config

        print("[couchbase] creating authenticator", flush=True)
        auth = PasswordAuthenticator(config.username, config.password)

        print(f"[couchbase] connecting to {config.connstr}", flush=True)
        self.cluster = Cluster.connect(
            config.connstr,
            ClusterOptions(auth),
        )

        print("[couchbase] waiting for cluster readiness", flush=True)
        self.cluster.wait_until_ready(
            timedelta(seconds=config.connect_timeout_seconds)
        )

        print(f"[couchbase] opening bucket {config.bucket}", flush=True)
        fares_bucket = self.cluster.bucket(config.bucket)

        print(f"[couchbase] opening scope {config.scope}", flush=True)
        fares_scope = fares_bucket.scope(config.scope)

        print(f"[couchbase] opening collection {config.current_collection}", flush=True)
        self.current = fares_scope.collection(config.current_collection)

        print(f"[couchbase] opening collection {config.batch_collection}", flush=True)
        self.batch = fares_scope.collection(config.batch_collection)

        print(f"[couchbase] opening collection {config.deadletter_collection}", flush=True)
        self.deadletter = fares_scope.collection(config.deadletter_collection)

        print(f"[couchbase] opening history bucket {config.history_bucket}", flush=True)
        history_bucket = self.cluster.bucket(config.history_bucket)

        print(f"[couchbase] opening history scope {config.history_scope}", flush=True)
        history_scope = history_bucket.scope(config.history_scope)

        print(f"[couchbase] opening history collection {config.history_collection}", flush=True)
        self.history = history_scope.collection(config.history_collection)

        self._test_collections()

        print("[couchbase] repository initialized", flush=True)

    def _test_collections(self) -> None:
        tests = [
            ("current_fares", self.current),
            ("batch_control", self.batch),
            ("dead_letter", self.deadletter),
            ("fare_changes_7d", self.history),
        ]

        for name, collection in tests:
            print(f"[couchbase] testing collection access: {name}", flush=True)
            try:
                collection.get("__connection_test__")
            except DocumentNotFoundException:
                print(f"[couchbase] collection reachable: {name}", flush=True)

    @staticmethod
    def content_as_dict(result: Any) -> dict[str, Any]:
        """
        Decode Couchbase document content into a dict.

        Normal case:
          result.content_as[dict] returns dict.

        Recovery case:
          an earlier version may have stored JSON as a string.
          If so, decode it back into a dict.
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

    def get_current(self, key: str) -> tuple[dict[str, Any], Any] | None:
        try:
            result = self.current.get(key)
            return self.content_as_dict(result), result.cas
        except DocumentNotFoundException:
            return None

    def insert_current(self, key: str, doc: dict[str, Any]) -> bool:
        try:
            print(f"[couchbase] inserting current: {key}", flush=True)
            self.current.insert(key, doc)
            return True
        except DocumentExistsException:
            return False

    def replace_current(self, key: str, doc: dict[str, Any], cas: Any) -> bool:
        try:
            print(f"[couchbase] replacing current: {key}", flush=True)
            self.current.replace(key, doc, ReplaceOptions(cas=cas))
            return True
        except CasMismatchException:
            return False

    def insert_history(self, key: str, doc: dict[str, Any]) -> bool:
        try:
            print(f"[couchbase] inserting history: {key}", flush=True)

            # TTL is intentionally not set here because the collection should have
            # maxTTL=691200 seconds. If you cannot set collection TTL, you can add
            # an InsertOptions(expiry=timedelta(days=8)) later.
            self.history.insert(key, doc)
            return True

        except DocumentExistsException:
            # Idempotent retry of the same batch/hash.
            print(f"[couchbase] history already exists: {key}", flush=True)
            return False

    def upsert_batch(self, key: str, doc: dict[str, Any]) -> None:
        print(f"[couchbase] upserting batch control: {key}", flush=True)
        self.batch.upsert(key, doc)

    def upsert_batch_control(self, key: str, doc: dict[str, Any]) -> None:
        self.upsert_batch(key, doc)

    def insert_deadletter(self, key: str, doc: dict[str, Any]) -> None:
        print(f"[couchbase] inserting dead letter: {key}", flush=True)
        self.deadletter.upsert(key, doc)

    def insert_dead_letter(self, key: str, doc: dict[str, Any]) -> None:
        self.insert_deadletter(key, doc)

    def close(self) -> None:
        try:
            self.cluster.close()
        except AttributeError:
            pass