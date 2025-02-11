#Imports
import json
import os
import pathlib
import sys
import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter

from kafka import KafkaConsumer
import utils.utils_config as config
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger
from utils.utils_producer import verify_services, is_topic_available

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_case import init_db, insert_message

# Store data for visualizations
author_counts = Counter()
category_counts = Counter()

# CSV file for storing positive messages
positive_messages_file = "positive_messages.csv"

# Initialize Matplotlib plots
plt.ion()
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
ax1, ax2 = axes

def update_visualizations():
    """
    Update visualizations dynamically with new data.
    """
    # Clear previous plots
    ax1.clear()
    ax2.clear()

    # Bar Chart for Author Message Count
    if author_counts:
        authors, counts = zip(*author_counts.most_common())
        ax1.bar(authors, counts, color='skyblue')
        ax1.set_title("Messages Per Author")
        ax1.set_ylabel("Message Count")
        ax1.set_xticklabels(authors, rotation=45, ha="right")

    # Pie Chart for Category Distribution
    if category_counts:
        categories, counts = zip(*category_counts.items())
        ax2.pie(counts, labels=categories, autopct='%1.1f%%', startangle=90)
        ax2.set_title("Category Distribution")

    # Draw updated plots
    plt.tight_layout()
    plt.draw()
    plt.pause(0.1)

def process_message(message: dict) -> None:
    """
    Process a single JSON message and update data for visualization.
    Store positive messages in a CSV file.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }

        # Update author message count
        author_counts[processed_message["author"]] += 1

        # Update category distribution
        category_counts[processed_message["category"]] += 1

        # Store positive messages in CSV
        if processed_message["sentiment"] > 0:
            store_positive_message(processed_message)

        # Update the visualizations with the new data
        update_visualizations()

        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

def store_positive_message(message: dict) -> None:
    """
    Store a positive message in the CSV file.
    """
    try:
        # Check if the CSV file exists, if not create it with headers
        if not os.path.exists(positive_messages_file):
            with open(positive_messages_file, mode='w', newline='', encoding='utf-8') as file:
                file.write("message,author,timestamp,category,sentiment,keyword_mentioned,message_length\n")

        # Append the positive message to the CSV file
        with open(positive_messages_file, mode='a', newline='', encoding='utf-8') as file:
            file.write(f'"{message["message"]}","{message["author"]}","{message["timestamp"]}","{message["category"]}",{message["sentiment"]},"{message["keyword_mentioned"]}",{message["message_length"]}\n')

        logger.info(f"Stored positive message: {message['message']}")

    except Exception as e:
        logger.error(f"Error storing positive message in CSV: {e}")

def consume_messages_from_kafka(topic, kafka_url, group, sql_path, interval_secs):
    """
    Consume new messages from Kafka topic and process them.
    """
    try:
        verify_services()
        consumer = create_kafka_consumer(
            topic, group, value_deserializer_provided=lambda x: json.loads(x.decode("utf-8"))
        )
        is_topic_available(topic)
    except Exception as e:
        logger.error(f"ERROR: Kafka initialization failed: {e}")
        sys.exit(11)

    if consumer is None:
        logger.error("ERROR: Consumer is None. Exiting.")
        sys.exit(13)

    try:
        for message in consumer:
            processed_message = process_message(message.value)
            if processed_message:
                insert_message(processed_message, sql_path)

    except Exception as e:
        logger.error(f"ERROR: Could not consume messages from Kafka: {e}")
        raise

def main():
    """
    Main function to run the consumer process.
    """
    try:
        topic = config.get_kafka_topic()
        kafka_url = config.get_kafka_broker_address()
        group_id = config.get_kafka_consumer_group_id()
        interval_secs = config.get_message_interval_seconds_as_int()
        sqlite_path = config.get_sqlite_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    try:
        consume_messages_from_kafka(topic, kafka_url, group_id, sqlite_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        logger.info("Consumer shutting down.")

if __name__ == "__main__":
    main()

