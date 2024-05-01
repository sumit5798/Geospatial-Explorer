# Geospatial explorer

**Dataset**: Google review dataset

## 1 Setting up the System

To set up the system, follow these steps:

Apache Kafka:

- Make sure Kafka is installed and accessible.
- Start the Kafka server using the appropriate command or script for your Kafka installation.

Consumer Side: The consumer is responsible for receiving data from the producer and ingesting it into Couchbase DB.

- Configure the Consumer script to connect to the same Kafka broker and topics as specified in the Producer.
- Run the Consumer script. It listens to the Kafka topic, consumes messages, parses JSON, and ingests data into Couchbase DB.

Producer Side: The Producer code processes data and sends it to Kafka. Make sure you have a running and accessible Kafka server.

A Distributed System Approach to Location-Based Service using Couchbase and Apache Kafka

- Set up and configure the Kafka Producer in your Producer script to connect to your Kafka broker. Ensure the Kafka topic exists or create it using a Kafka command-line tool or programmatically using a library like Kafka-python.
- Run the Producer script after setting up the Kafka connection. This script processes the data and sends the results to the specified Kafka topic.

## 2 Workflow

The system analyzes California Google Local Data and calculates average ratings for 'gmapid' entities. Apache Kafka acts as a middleman, receiving, storing, and distributing structured data across topics for downstream actions. The Consumer reads data from Kafka, decodes and processes it, and updates cumulative ratings for 'gmapid' entities in a synchronized dictionary. Couchbase, the NoSQL database, easily integrates and efficiently saves the processed information, including cumulative and average ratings for 'gmapid' entities.

The aggregated information stored in Couchbase is then used for further analysis, such as providing Place Recommendations based on User Rating using the geospatial information (latitude and longitude) stored in Couchbase. Various parameters are computed, which are discussed in the results section of this project.

## 3 RESULTS

This section presents various studies, including spatial visualization of top-rated stores in California, displaying rating ranges through charts, and assessing business distances from specific locations. Python modules like matplotlib, folium, and haversine are used to loop through query results and display them as Geospatial Maps.

It showcases real-world application scenarios of the distributed system, such as recommending highly rated establishments and calculating distances between businesses and specified areas.
