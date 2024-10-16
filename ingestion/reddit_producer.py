import time
from integration.api.config import reddit_client_id, reddit_client_secret, reddit_user_agent, \
    kafka_bootstrap_servers, crypto_list_file, last_processed_timestamp_file, topic_name
import praw
from kafka import KafkaProducer
import json
import csv


# Setup reddit client
reddit = praw.Reddit(
    client_id=reddit_client_id,
    client_secret=reddit_client_secret,
    user_agent=reddit_user_agent
)

# Create Kafka Producer instance
producer = KafkaProducer(
    bootstrap_servers=kafka_bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


# Read from Crypto List
with open(crypto_list_file, "r", encoding="cp437") as crypto_file:
    crypto_list = []
    count = 0
    next(crypto_file)
    read_crypto_file = csv.reader(crypto_file)
    for row in read_crypto_file:
        crypto_list.append(row[2])
        #count += 1
        #if count == 20:
        #    break


crypto_keywords = ["crypto", "coin", "token", "blockchain", "cryptocurrency", "wallet"]


def is_crypto_related(content):
    """
    To see if the reddit post is crypto related.
    :param content: Reddit post that needs to be checked
    :return: True or False
    """
    # Convert content to lowercase to make sure the search is case-insensitive
    content_lower = content.lower()
    for keyword in crypto_keywords:
        if keyword in content_lower:
            return True
        else:
            return False


def on_send_success(record_metadata):
    print(f"Message sent to topic {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")


def on_send_error(excp):
    print(f"Error sending message: {excp}")


# Variable to store last processed timestamp
with open(last_processed_timestamp_file, "r") as file:
    last_processed_timestamp = float(file.read())


# Function to fetch and print posts related to cryptocurrencies
def fetch_crypto_posts(subreddit_name="all", limit=10):
    global last_processed_timestamp
    max_timestamp = last_processed_timestamp
    try:
        for crypto in crypto_list:
            print(f"Fetching posts related to {crypto}")
            query = f'title:"{crypto}" OR subreddit:"{crypto}"'

            # Fetch posts based on the crypto keyword
            for submission in reddit.subreddit(subreddit_name).search(query, sort='new', limit=limit):
                post_timestamp = submission.created_utc

                # Skip post if it is old
                if post_timestamp <= last_processed_timestamp:
                    continue

                # Extract relevant post information
                post_data = {
                    "crypto": crypto,
                    "title": submission.title,
                    "subreddit": submission.subreddit.display_name,
                    "content": submission.selftext,
                    "created_utc": submission.created_utc
                }

                # Check if the post is crypto-related based on title or content
                if is_crypto_related(post_data['title']) or is_crypto_related(post_data['content']):
                    # Send to kafka topic
                    producer.send(topic_name, post_data).add_callback(on_send_success).add_errback(on_send_error)
                    producer.flush()
                # Update max timestamp
                max_timestamp = max(max_timestamp, post_data['created_utc'])

        with open(last_processed_timestamp_file, 'w') as file:
            file.write(str(max_timestamp))

        # Wait for a minute before fetching again
        time.sleep(60)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    while True:
        fetch_crypto_posts(limit=10)
