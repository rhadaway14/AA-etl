import os
from datetime import timedelta

from dotenv import load_dotenv
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.exceptions import DocumentNotFoundException

load_dotenv()

connstr = os.getenv("COUCHBASE_CONNSTR")
username = os.getenv("COUCHBASE_USERNAME")
password = os.getenv("COUCHBASE_PASSWORD")

print("Connection string:", connstr)
print("Username:", username)
print("Password loaded:", bool(password))

if not connstr or not username or not password:
    raise RuntimeError("Missing COUCHBASE_CONNSTR, COUCHBASE_USERNAME, or COUCHBASE_PASSWORD")

auth = PasswordAuthenticator(username, password)

cluster = Cluster.connect(
    connstr,
    ClusterOptions(auth),
)

cluster.wait_until_ready(timedelta(seconds=30))

bucket = cluster.bucket("fares")
scope = bucket.scope("airline")
collection = scope.collection("current_fares")

try:
    collection.get("__connection_test__")
except DocumentNotFoundException:
    print("Connected successfully. Bucket/scope/collection are reachable.")