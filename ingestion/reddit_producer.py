from integration.api.config import reddit_client_id, reddit_client_secret, reddit_user_agent, \
    kafka_bootstrap_servers, crypto_list_file
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
    count=0
    next(crypto_file)
    read_crypto_file = csv.reader(crypto_file)
    for row in read_crypto_file:
        crypto_list.append(row[2])
        count += 1
        if count == 2:
            break


# Keywords that indicate crypto reference
crypto_keywords = ["crypto", "coin", "blockchain", "token"]


# Function to fetch and print posts related to cryptocurrencies
def fetch_crypto_posts(subreddit_name="all", limit=10):
    posts = []
    try:
        for crypto in crypto_list:
            print(f"Fetching posts related to {crypto}")
            for submission in reddit.subreddit(subreddit_name).search(crypto, limit=limit):
                # Extract relevant post information
                post_data = {
                    "title": submission.title,
                    "subreddit": submission.subreddit.display_name,
                    "content": submission.selftext
                }
                # Print post details
                print(f"Title: {post_data['title']}")
                print(f"Subreddit: {post_data['subreddit']}")
                print(f"Content: {post_data['content']}")
                posts.append(post_data)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    fetch_crypto_posts(limit=5)
