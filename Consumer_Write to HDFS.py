# saves the received articles in HDFS directory

from confluent_kafka import Consumer, KafkaException
from hdfs import InsecureClient
import json

# Kafka broker address
kafka_broker = "localhost:9092"  # Replace with your Kafka broker address

# Kafka topic to consume messages from
kafka_topic = "NewsAnalysis"

# HDFS configuration
hdfs_namenode = "hdfs://data-integration-m"  # Replace with your HDFS Namenode address
hdfs_user = "geedhu92"  # Replace with your Hadoop username
hdfs_path = "hdfs://data-integration-m:9870/NewsAnalysis_Hadoop_JDG/news_data.json"

# Set up Kafka consumer configuration
consumer_config = {
    "bootstrap.servers": kafka_broker,
    "group.id": "1",
    "auto.offset.reset": "earliest"  # Start reading from the beginning of the topic
}

# Set up HDFS client
hdfs_client = InsecureClient(hdfs_namenode, user=hdfs_user)

# Create Kafka consumer instance
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe([kafka_topic])

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Decode the message value from JSON
        article_json = msg.value().decode("utf-8")
        article = json.loads(article_json)

        # Save the article data to HDFS
        with hdfs_client.write(hdfs_path, encoding="utf-8", overwrite=True) as hdfs_file:
            json.dump(article, hdfs_file, indent=2)

except KeyboardInterrupt:
    pass
finally:
    # Close the consumer
    consumer.close()
