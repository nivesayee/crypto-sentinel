from integration.api.config import reddit_client_id, reddit_client_secret, reddit_user_agent, \
    kafka_bootstrap_servers, crypto_list_file, reddit_posts_file
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


# Function to fetch and print posts related to cryptocurrencies
def fetch_crypto_posts(subreddit_name="all", limit=10):
    try:
        # Open the file in write mode (with headers) once at the beginning
        with open(reddit_posts_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=["crypto", "title", "subreddit", "content"])
            writer.writeheader()  # Write the header only once

            for crypto in crypto_list:
                print(f"Fetching posts related to {crypto}")
                query = f'title:"{crypto}" OR subreddit:"{crypto}"'

                # Fetch posts based on the crypto keyword
                for submission in reddit.subreddit(subreddit_name).search(query, limit=limit):
                    # Extract relevant post information
                    post_data = {
                        "crypto": crypto,
                        "title": submission.title,
                        "subreddit": submission.subreddit.display_name,
                        "content": submission.selftext
                    }

                    # Check if the post is crypto-related based on title or content
                    if is_crypto_related(post_data['title']) or is_crypto_related(post_data['content']):
                        # Write the post data directly to the file
                        writer.writerow(post_data)

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    fetch_crypto_posts(limit=50)
