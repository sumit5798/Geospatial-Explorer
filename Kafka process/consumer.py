from datetime import timedelta
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.options import ClusterOptions, QueryOptions, ClusterTimeoutOptions
from couchbase.exceptions import AmbiguousTimeoutException
from kafka import KafkaConsumer
import json
import uuid
from threading import Thread

def consume_messages(consumer, collection):
    count = 0
    try:
        for record in consumer:
            msg = record.value
            count += 1

            try:
                doc = json.loads(msg)
                id = str(uuid.uuid4())

                # Use the Collection class to interact with the Couchbase bucket
                upsert_with_retry(collection, id, doc)
            except json.JSONDecodeError as ex:
                print("Not a JSON object: " + str(ex))

        print(count)

    except Exception as ex:
        print("EXCEPTION!!!!" + str(ex))

def upsert_with_retry(collection, id, doc, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            collection.upsert(id, doc)
            return  # Operation succeeded, exit the loop
        except AmbiguousTimeoutException as ex:
            print(f"Retry {retries + 1} - Upsert failed: {ex}")
            retries += 1
        except Exception as ex:
            print(f"Unexpected error during upsert: {ex}")
            break  # Exit the loop on unexpected errors

    print(f"Failed to upsert after {max_retries} retries.")
    
def main():
    # Set up Couchbase connection (unchanged)
    timeout_options=ClusterTimeoutOptions(kv_timeout=timedelta(seconds=600), query_timeout=timedelta(seconds=600))
    options=ClusterOptions(PasswordAuthenticator('admin', 'password'), timeout_options=timeout_options)
    cluster = Cluster.connect('couchbase://localhost',options)

    bucket = cluster.bucket('host')
    collection = bucket.default_collection()

    # Create and start multiple consumer threads
    num_consumer_threads = 4  # Adjust this based on the desired number of consumer instances
    consumer_threads = []

    for _ in range(num_consumer_threads):
        # Set up Kafka consumer inside the loop to create a new instance for each thread
        consumer = KafkaConsumer(
            'googleReviewTopic',
            bootstrap_servers='localhost:9092',
            group_id='my_consumer_group',  # Use a unique group ID for each consumer instance
            value_deserializer=lambda x: x.decode('utf-8'),
            max_poll_records=500,           # Adjust based on your system's capacity
            max_poll_interval_ms=600000     # Adjust based on the maximum processing time per batch
        )

        thread = Thread(target=consume_messages, args=(consumer, collection))
        consumer_threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in consumer_threads:
        thread.join()

    # Clean up resources
    cluster.disconnect()

if __name__ == "__main__":
    main()
