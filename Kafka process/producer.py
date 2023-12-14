from kafka import KafkaProducer
import json

def main():
    bootstrap_servers = 'localhost:9092'
    topic = 'googleReviewTopic'

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        batch_size=16384,  # Adjust the batch size based on your system's capacity
        linger_ms=5        # Adjust the linger time for batching
    )

    # Load metadata
    metadata_file_path = 'C:/Users/sumit/OneDrive/Desktop/Study/DSD/Project/COMP-6231-Project/meta-New_Mexico.json'
    with open(metadata_file_path, 'r') as metadata_file:
        metadata_list = [json.loads(line) for line in metadata_file]

    # Dictionary to store gmap_id and its average rating
    gmap_ratings = {}

    input_file_path = 'C:/Users/sumit/OneDrive/Desktop/Study/DSD/Project/COMP-6231-Project/review-New_Mexico.json'  # Update with the actual path

    with open(input_file_path, 'r') as file:
        for line in file:
            jsons = line.strip()
            print("Processing line: " + jsons)

            if jsons:
                print("Splitting file chunk to jsons....")
                splitted_jsons = split(jsons)

                print("Converting to JsonDocuments....")

                doc_count = len(splitted_jsons)

                print("Number of documents is: " + str(doc_count))

                print("Calculating average ratings and sending messages to Kafka....")
                count = 0
                for doc in splitted_jsons:
                    print("sending msg...." + str(count))

                    # Calculate average rating and store in the dictionary
                    review_data = json.loads(doc)
                    gmap_id = review_data.get('gmap_id')
                    rating = review_data.get('rating')
                    if rating is None:
                        rating = 0
                    if gmap_id in gmap_ratings:
                        gmap_ratings[gmap_id]['total_rating'] += rating
                        gmap_ratings[gmap_id]['num_reviews'] += 1
                    else:
                        gmap_ratings[gmap_id] = {
                            'total_rating': rating,
                            'num_reviews': 1
                        }

                    count += 1

                print("Total of " + str(count) + " messages sent")

    for gmap_id, rating_data in gmap_ratings.items():
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

if __name__ == "__main__":
    main()
