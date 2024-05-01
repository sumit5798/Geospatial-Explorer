# needed for any cluster connection
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
# options for a cluster and SQL++ (N1QL) queries
from couchbase.options import ClusterOptions, QueryOptions
import json

def upsert_documents():
    """
    Upserts documents from a JSON file into a Couchbase bucket.

    This function connects to a Couchbase cluster, opens a bucket, and reads a JSON file.
    Each line in the JSON file is parsed as a document and upserted into the bucket.

    Raises:
        json.JSONDecodeError: If there is an error decoding the JSON.

    Returns:
        None
    """
    # get a reference to our cluster
    auth = PasswordAuthenticator('Admin', 'password')
    cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth))

    # Open the bucket
    bucket = cluster.bucket('DS_bucket')
    collection = bucket.default_collection()

    # Read JSON file and insert documents
    with open('meta-California.json', 'r') as file:
        for line in file:
            try:
                document = json.loads(line)
                document_id = document.get('gmap_id')  # Use a unique identifier as the document ID
                collection.upsert(document_id, document)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
