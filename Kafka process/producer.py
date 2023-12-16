import multiprocessing
import os
import sys
from kafka import KafkaProducer
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager, Queue
import json
from functools import partial

def load_balancer(input_file_path):
    print("load balncer")
    # Read lines from the file
    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    # Get the number of Docker instances and total lines
    docker_instance = int(os.environ.get('DOCKER_INSTANCE', 1))
    total_lines = len(lines)

    # Calculate lines per instance
    lines_per_instance = total_lines // 2

    # Calculate start and end indices for the current Docker instance
    start_index = (docker_instance - 1) * lines_per_instance
    end_index = min(docker_instance * lines_per_instance, total_lines)
    print(start_index)
    print(end_index)
    # Select the lines for the current chunk
    lines_to_process = lines[start_index:end_index]

    return lines_to_process

def split(json_str):
    splitted_json_elements = []

    # Check if the input json_str is not empty
    if json_str.strip():
        try:
            # Attempt to load the JSON
            json_data = json.loads(json_str)

            # If it's a list, treat each element separately
            if isinstance(json_data, list):
                for individual_element in json_data:
                    splitted_json_elements.append(json.dumps(individual_element))
            else:
                # If it's a single object, treat it as is
                splitted_json_elements.append(json_str)
        except json.JSONDecodeError as ex:
            print("Error decoding JSON: " + str(ex))

    return splitted_json_elements

def process_line(shared_ratings, lock, line_chunks):
    # Process each line in the chunk
    for line in line_chunks:
        jsons = line.strip()
        print("Processing line: " + jsons)

        if jsons:
            # print("Splitting file chunk to jsons....")
            splitted_jsons = split(jsons)

            # print("Converting to JsonDocuments....")

            doc_count = len(splitted_jsons)

            # print("Number of documents is: " + str(doc_count))

            # print("Calculating average ratings....")
            for doc in splitted_jsons:
                # Calculate average rating and store in the shared dictionary
                review_data = json.loads(doc)
                gmap_id = review_data.get('gmap_id')
                rating = review_data.get('rating')
                if rating is None:
                    rating = 0
                with lock:
                    if gmap_id in shared_ratings:
                        shared_ratings[gmap_id] = {
                            'total_rating': rating + shared_ratings[gmap_id]["total_rating"],
                            'num_reviews': 1 + shared_ratings[gmap_id]["num_reviews"]
                        }
                        print(f'gmap: {gmap_id}, rating:{shared_ratings[gmap_id]["total_rating"]}, num_reviews:{shared_ratings[gmap_id]["num_reviews"]}')
                    else:
                        shared_ratings[gmap_id] = {
                            'total_rating': rating,
                            'num_reviews': 1
                        }
                        print(f'gmap: {gmap_id}, rating:{shared_ratings[gmap_id]["total_rating"]}, num_reviews:{shared_ratings[gmap_id]["num_reviews"]}')

def main():
    input_file_path = 'review-New_Mexico.json'
    metadata_file_path = 'meta-New_Mexico.json'
    num_processes = 2

    with open(metadata_file_path, 'r') as metadata_file:
        metadata_list = [json.loads(line) for line in metadata_file]
        
    print("b4 load balncer")
    # Select the lines for the current chunk
    line_chunks = load_balancer(input_file_path)

    print("after load balncer")

    # Adjust the chunk size according to the number of lines you want to process in parallel
    chunk_size = len(line_chunks) // num_processes

    # Split the lines into chunks of the specified size
    line_chunks_process = [line_chunks[i:i + chunk_size] for i in range(0, len(line_chunks), chunk_size)]

    print(line_chunks_process)

    # Use a multiprocessing Manager to create a shared dictionary
    with Manager() as manager:
        shared_ratings = manager.dict()
        lock = manager.Lock()

        process_line_partial = partial(process_line, shared_ratings, lock)

        with ProcessPoolExecutor() as executor:
            # Pass the partially-applied function to the map
            executor.map(process_line_partial, line_chunks_process)

        # Wait for all processes to complete
        executor.shutdown(wait=True)

        # Send data to Kafka after processing all chunks
        bootstrap_servers = '192.168.2.19:9092'
        topic = 'ExTopic'
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            batch_size=16384,
            linger_ms=5,
            retries=3,  # Optional: You can adjust the number of retries
            acks='all',  # Optional: You can adjust the number of acknowledgments
            # Add the following line to increase metadata update timeout
            metadata_max_age_ms=60000  # Set the value in milliseconds (60 seconds in this example)
        )

        # Process the shared ratings and send to Kafka
        for gmap_id, rating_data in shared_ratings.items():
            avg_rating = rating_data['total_rating'] / rating_data['num_reviews']
            metadata_info = next((item for item in metadata_list if item['gmap_id'] == gmap_id), None)
            if metadata_info:
                json_object = {
                    'gmap_id': gmap_id,
                    'avg_rating': avg_rating,
                    'latitude': metadata_info['latitude'],
                    'longitude': metadata_info['longitude'],
                    'business_name': metadata_info['name']
                }
                json_string = json.dumps(json_object, indent=2)
                # Send the JSON object to Kafka
                producer.send(topic, value=json_string.encode('utf-8'))
                print("Output data sent to Kafka")

        producer.close()


if __name__ == "__main__":
    main()
