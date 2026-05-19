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

history = (
    cluster
    .bucket("fare_history")
    .scope("airline")
    .collection("fare_changes_7d")
)

key = "connection-test::history::" + datetime.now(timezone.utc).isoformat()

doc = {
    "type": "connection_test",
    "created_at": datetime.now(timezone.utc).isoformat(),
}

print("Inserting:", key, flush=True)
history.insert(key, doc)
print("History insert succeeded.", flush=True)