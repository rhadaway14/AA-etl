from dataclasses import dataclass
import os


@dataclass(frozen=True)
class CouchbaseConfig:
    connstr: str
    username: str
    password: str

    bucket: str = "fares"
    scope: str = "airline"
    current_collection: str = "current_fares"
    batch_collection: str = "batch_control"
    deadletter_collection: str = "dead_letter"

    history_bucket: str = "fare_history"
    history_scope: str = "airline"
    history_collection: str = "fare_changes_7d"

    connect_timeout_seconds: int = 20
    kv_timeout_seconds: int = 10

    @staticmethod
    def from_env() -> "CouchbaseConfig":
        required = [
            "COUCHBASE_CONNSTR",
            "COUCHBASE_USERNAME",
            "COUCHBASE_PASSWORD",
        ]

        missing = [name for name in required if not os.getenv(name)]

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}"
            )

        return CouchbaseConfig(
            connstr=os.environ["COUCHBASE_CONNSTR"],
            username=os.environ["COUCHBASE_USERNAME"],
            password=os.environ["COUCHBASE_PASSWORD"],

            bucket=os.getenv("COUCHBASE_BUCKET", "fares"),
            scope=os.getenv("COUCHBASE_SCOPE", "airline"),
            current_collection=os.getenv(
                "COUCHBASE_CURRENT_COLLECTION",
                "current_fares",
            ),
            batch_collection=os.getenv(
                "COUCHBASE_BATCH_COLLECTION",
                "batch_control",
            ),
            deadletter_collection=os.getenv(
                "COUCHBASE_DEADLETTER_COLLECTION",
                "dead_letter",
            ),

            history_bucket=os.getenv("COUCHBASE_HISTORY_BUCKET", "fare_history"),
            history_scope=os.getenv("COUCHBASE_HISTORY_SCOPE", "airline"),
            history_collection=os.getenv(
                "COUCHBASE_HISTORY_COLLECTION",
                "fare_changes_7d",
            ),

            connect_timeout_seconds=int(
                os.getenv("COUCHBASE_CONNECT_TIMEOUT_SECONDS", "20")
            ),
            kv_timeout_seconds=int(
                os.getenv("COUCHBASE_KV_TIMEOUT_SECONDS", "10")
            ),
        )