import os
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

load_dotenv()

cluster = Cluster.connect(
    os.getenv("COUCHBASE_CONNSTR"),
    ClusterOptions(
        PasswordAuthenticator(
            os.getenv("COUCHBASE_USERNAME"),
            os.getenv("COUCHBASE_PASSWORD"),
        )
    ),
)

cluster.wait_until_ready(timedelta(seconds=30))

tests = [
    ("fares", "airline", "current_fares"),
    ("fares", "airline", "batch_control"),
    ("fares", "airline", "dead_letter"),
    ("fare_history", "airline", "fare_changes_7d"),
]

for bucket_name, scope_name, collection_name in tests:
    print(f"\nTesting {bucket_name}.{scope_name}.{collection_name}", flush=True)

    collection = (
        cluster
        .bucket(bucket_name)
        .scope(scope_name)
        .collection(collection_name)
    )

    key = (
        "connection-test::"
        + bucket_name
        + "::"
        + scope_name
        + "::"
        + collection_name
        + "::"
        + datetime.now(timezone.utc).isoformat()
    )

    doc = {
        "type": "connection_test",
        "bucket": bucket_name,
        "scope": scope_name,
        "collection": collection_name,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    print(f"Inserting {key}", flush=True)
    collection.insert(key, doc)
    print("Insert succeeded.", flush=True)

print("\nAll collection inserts succeeded.")