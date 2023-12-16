from datetime import timedelta
from multiprocessing import Manager
import threading
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.options import ClusterOptions, QueryOptions, ClusterTimeoutOptions
from couchbase.exceptions import AmbiguousTimeoutException
from kafka import KafkaConsumer
import json
import uuid
from threading import Thread, Lock

def consume_messages(consumer, shared_collection, lock, ratings_dict):
    print("Consume messages")
    try:
        print("Inside try block")
        count = 0

        while True:
            # Poll for records with a timeout of 0, which means non-blocking
            records = consumer.poll(timeout_ms=1800*1000)

            # Check if there are any records
            if not records:
                break  # No records, exit the loop

            for record in records.values():
                for msg in record:
                    count += 1
                    try:
                        print("Inside try block2")
                        doc = json.loads(msg.value)
                        gmap_id = doc.get('gmap_id')
                        if gmap_id is not None:
                            id = str(gmap_id)
                            # Acquire the lock before modifying the ratings_dict
                            with lock:
                                if id in ratings_dict:
                                    # Update cumulative values and count for existing gmap_id
                                    ratings_dict[id]['sum'] += doc['avg_rating']
                                    ratings_dict[id]['count'] += 1
                                else:
                                    # Initialize values for new gmap_id
                                    ratings_dict[id] = {'sum': doc['avg_rating'], 'count': 1}
                                    shared_collection.append((id, doc))
                                print(shared_collection)
                        else:
                            print("JSON object does not contain 'gmap_id'")

                    except json.JSONDecodeError as ex:
                        print("Not a JSON object: " + str(ex))

        print(f"Thread {threading.current_thread().name} processed {count} messages.")

    except Exception as ex:
        print(f"EXCEPTION in Thread {threading.current_thread().name}: {ex}")

def calculate_average_ratings(ratings_dict):
    # Calculate average ratings for each gmap_id
    for id, values in ratings_dict.items():
        average_rating = values['sum'] / values['count']
        values['average_rating'] = average_rating

def upsert_with_retry(collection, documents, lock, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            # Acquire the lock before iterating over the shared collection
            with lock:
                # Iterate over the shared collection and upsert each document
                for id, doc in documents:
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
    shared_collection = []
    lock = Lock()

    # Create a shared manager
    with Manager() as manager:
        # Create a shared dictionary for ratings
        ratings_dict = manager.dict()

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

            thread = Thread(target=consume_messages, args=(consumer, shared_collection, lock, ratings_dict))
            consumer_threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in consumer_threads:
            thread.join()

        # Calculate average ratings before calling upsert_with_retry
        calculate_average_ratings(ratings_dict)

        # Call upsert_with_retry with the shared collection and lock
        upsert_with_retry(cluster.bucket('host').default_collection(), shared_collection, lock)

    # Clean up resources
    cluster.close()

if __name__ == "__main__":
    main()
