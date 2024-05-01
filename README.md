# Geospatial explorer

This repository contains code for building a data pipeline that streams data from a Kafka topic into a Couchbase bucket. It consists of three main components: a Kafka producer (`producer.py`), a Kafka consumer (`consumer.py`), and a script for upserting documents into Couchbase (`CBUpsert.py`). Additionally, there's a Jupyter Notebook (`MapImpl.ipynb`) demonstrating data retrieval and visualization from Couchbase.

## Setup

### Requirements

- Python 3.x
- Apache Kafka
- Couchbase Server

### Installation

1. Install the required Python packages:

   ```bash
   pip install kafka-python couchbase folium matplotlib
   ```

2. Configure Kafka and Couchbase server details in the respective scripts (producer.py, consumer.py, CBUpsert.py, MapImpl.ipynb).

### Components

#### Producer (producer.py)

The producer.py script reads data from a JSON file, splits it into individual JSON documents, and publishes each document to a Kafka topic.

#### Consumer (consumer.py)

The consumer.py script consumes messages from a Kafka topic, parses them as JSON objects, generates a unique ID for each message, and upserts the message document into a Couchbase bucket.

#### Couchbase Upsert (CBUpsert.py)

The CBUpsert.py script upserts documents from a JSON file into a Couchbase bucket. Each line in the JSON file is treated as a separate document and upserted into the bucket.

#### Map Implementation (MapImpl.ipynb)

The MapImpl.ipynb Jupyter Notebook demonstrates data retrieval from Couchbase and visualization on a map using Folium. It also includes examples of querying Couchbase using N1QL.

### Usage

1. Start Apache Kafka and Couchbase Server.
2. Configure Kafka and Couchbase server details in the scripts.
3. Run the producer script to publish data to Kafka.
4. Run the consumer script to consume data from Kafka and store it in Couchbase.
5. Use the provided Jupyter Notebook to interact with Couchbase and visualize data.

### Note

Make sure to replace placeholder values for Kafka bootstrap servers, Couchbase cluster URL, authentication credentials, and file paths with actual values specific to your environment.
