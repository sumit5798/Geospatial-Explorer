from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.bucket import Bucket
from couchbase.options import ClusterOptions, QueryOptions
from kafka import KafkaConsumer
import json
import uuid

def main():
    """
    Main function that consumes messages from a Kafka topic and stores them in a Couchbase bucket.

    This function initializes a Kafka consumer, connects to a Couchbase cluster, and retrieves a bucket and collection
    to store the consumed messages. It then enters a loop to consume messages from the Kafka topic, parse them as JSON,
    generate a unique ID for each message, and upsert the message document into the Couchbase collection.

    If a message is not a valid JSON object, it prints an error message. If any exception occurs during the process,
    it prints an exception message. Finally, it disconnects from the Couchbase cluster.

    Note: Make sure to replace the placeholder values for Kafka bootstrap servers, Couchbase cluster URL, and
    authentication credentials with the actual values specific to your environment.
    """
    consumer = KafkaConsumer(
        'googleReviewTopic',
        bootstrap_servers='localhost:9092',
        group_id='default',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    auth = PasswordAuthenticator('admin', 'password')
    cluster = Cluster.connect('couchbase://localhost', ClusterOptions(auth))

    bucket = cluster.bucket('reviews_bucket')
    collection = bucket.default_collection()

    try:
        for record in consumer:
            msg = record.value
            print(record.topic + ": " + msg)

            try:
                doc = json.loads(msg)
                id = str(uuid.uuid4())
                # Use the Collection class to interact with the Couchbase bucket
                collection.upsert(id, doc)
            except json.JSONDecodeError as ex:
                print("Not a JSON object: " + str(ex))

    except Exception as ex:
        print("EXCEPTION!!!!" + str(ex))
    finally:
        cluster.disconnect()

if __name__ == "__main__":
    main()
