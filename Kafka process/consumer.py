from datetime import timedelta
import threading
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.options import ClusterOptions, QueryOptions, ClusterTimeoutOptions
from couchbase.exceptions import AmbiguousTimeoutException
from kafka import KafkaConsumer
import json
import uuid
from threading import Thread

def consume_messages(consumer, cluster, bucket_name):
    print("Consume messages")
    try:
        print("Inside try block")
        # Create a Couchbase bucket for each consumer thread
        bucket = cluster.bucket(bucket_name)
        collection = bucket.default_collection()

        count = 0
        for record in consumer:
            print("Inside try block1")
            msg = record.value
            count += 1

            try:
                print("Inside try block2")
                doc = json.loads(msg)
                id = str(uuid.uuid4())

                # Use the Collection class to interact with the Couchbase bucket
                upsert_with_retry(collection, id, doc)
            except json.JSONDecodeError as ex:
                print("Not a JSON object: " + str(ex))

        print(f"Thread {threading.current_thread().name} processed {count} messages.")

    except Exception as ex:
        print(f"EXCEPTION in Thread {threading.current_thread().name}: {ex}")


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
    timeout_options = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=600), query_timeout=timedelta(seconds=600))
    options = ClusterOptions(PasswordAuthenticator('admin', 'password'), timeout_options=timeout_options)
    cluster = Cluster.connect('couchbase://localhost', options)

    # Create and start multiple consumer threads
    num_consumer_threads = 4  # Adjust this based on the desired number of consumer instances
    consumer_threads = []

    for i in range(num_consumer_threads):
        # Set up Kafka consumer inside the loop to create a new instance for each thread
        consumer = KafkaConsumer(
            'ExTopic',
            bootstrap_servers='localhost:9092',
            group_id=f'my_consumer_group',  # Use a unique group ID for each consumer instance
            value_deserializer=lambda x: x.decode('utf-8'),
            max_poll_records=10000,         # Adjust based on your system's capacity
            max_poll_interval_ms=600000     # Adjust based on the maximum processing time per batch
        )

        thread = Thread(target=consume_messages, args=(consumer, cluster, 'host'))
        consumer_threads.append(thread)
        thread.start()

    # Wait for all threads to finish
    for thread in consumer_threads:
        thread.join()

    # Clean up resources
    cluster.disconnect()

if __name__ == "__main__":
    main()
