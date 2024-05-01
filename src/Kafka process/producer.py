from kafka import KafkaProducer
import json

def main():
    """
    Main function that processes a file, splits it into JSON documents, and sends them to a Kafka topic.

    Args:
        None

    Returns:
        None
    """
    bootstrap_servers = 'localhost:9092'
    topic = 'googleReviewTopic'

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    input_file_path = 'C:/Users/sumit/OneDrive/Desktop/Study/DSD/Project/COMP-6231-Project/meta-California.json'

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

                print("Starting sending msg to kafka....")
                count = 0
                for doc in splitted_jsons:
                    print("sending msg...." + str(count))
                    producer.send(topic, value=doc.encode('utf-8'))
                    print("msg sent...." + str(count))

                    count += 1

                print("Total of " + str(count) + " messages sent")

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
