import multiprocessing
from kafka import KafkaProducer
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager, Queue
import json
from functools import partial

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

def process_line(shared_ratings, line_chunks):
    # Process each line in the chunk
    for line in line_chunks:
        jsons = line.strip()
        #print("Processing line: " + jsons)

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
                with multiprocessing.Lock():
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

def main():
    input_file_path = 'C:/Users/sumit/OneDrive/Desktop/Study/DSD/Project/COMP-6231-Project/review-New_Mexico.json'  # Update with the actual path
    # Load metadata
    metadata_file_path = 'C:/Users/sumit/OneDrive/Desktop/Study/DSD/Project/COMP-6231-Project/meta-New_Mexico.json'
    with open(metadata_file_path, 'r') as metadata_file:
        metadata_list = [json.loads(line) for line in metadata_file]
        
    with open(input_file_path, 'r') as file:
        lines = file.readlines()

    # Adjust the chunk size according to the number of lines you want to process in parallel
    chunk_size = 10000

    # Split the lines into chunks of the specified size
    line_chunks = [lines[i:i + chunk_size] for i in range(0, len(lines), chunk_size)]

    # Use a multiprocessing Manager to create a shared dictionary
    with Manager() as manager:
        shared_ratings = manager.dict()

        process_line_partial = partial(process_line, shared_ratings)

        with ProcessPoolExecutor() as executor:
            # Pass the partially-applied function to the map
            executor.map(process_line_partial, line_chunks)

        # Wait for all processes to complete
        executor.shutdown(wait=True)

        # Send data to Kafka after processing all chunks
        bootstrap_servers = 'localhost:9092'
        topic = 'googleReviewTopic'
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            batch_size=16384,
            linger_ms=5
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
