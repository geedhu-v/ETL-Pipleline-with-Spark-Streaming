from kafka import KafkaConsumer
import json

# Kafka Configuration
brokers = 'localhost:9092'
topic = 'NewsAnalysis'


# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    auto_offset_reset='earliest',  # Start from the earliest messages
    group_id='my-group'  # Consumer group ID
)

# Function to save data to HDFS
def save_to_hdfs(data, path):
    # Check if the file exists. If it does, overwrite it; if not, create a new one
    if hdfs_client.status(path, strict=False):
        # File exists, append
        with hdfs_client.write(path, append=True, encoding='utf-8') as writer:
            writer.write(data + '\n')
    else:
        # File doesn't exist, create new
        with hdfs_client.write(path, encoding='utf-8') as writer:
            writer.write(data + '\n')

# Keep track of the last written article
last_article = None

# Consumer loop that saves data to HDFS
for message in consumer:
    article = json.loads(message.value)  # Assuming the message is a JSON string

    # Check if the new article is the same as the last written one
    if article != last_article:
        # Filter out articles with specific conditions
        if article.get("url") != "https://removed.com" and article.get("description") != "[Removed]":
            # Clean and structure the data into columns
            cleaned_data = {
                "title": article.get("title", "No Title"),
                "description": article.get("description", "No Description"),
                "publishedAt": article.get("publishedAt", ""),
                "source_name": article.get("source", {}).get("name", "Unknown"),
                "url": article.get("url", ""),
                "urlToImage": article.get("urlToImage", ""),
                "content": article.get("content","No Content")
            }

            # Convert the cleaned data to a JSON string
            cleaned_json = json.dumps(cleaned_data)

            # Save the cleaned data to HDFS
            hdfs_path = '/BigData/FinalProject/news_JDG.json'  # Change to your HDFS path
            save_to_hdfs(cleaned_json, hdfs_path)

        # Update the last written article
        last_article = article
    else:
        print(f"Skipping duplicate article: {article.get('title', 'No Title')}")