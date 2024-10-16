import time
from integration.api.config import kafka_bootstrap_servers, topic_name, influx_db, \
    influx_token, influx_org, influx_url
import json
from textblob import TextBlob
from kafka import KafkaConsumer
from influxdb_client import InfluxDBClient

# Create Kafka Consumer instance
consumer = KafkaConsumer(topic_name, bootstrap_server=kafka_bootstrap_servers,
                         auto_offset_reset="earliest",
                         enable_auto_commit=True,
                         value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Influx DB client setup
influx_client = InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
influx_client.switch_database(influx_db)


def analyse_sentiment(post_content):
    """
    Function to analyse sentiment of post
    :param post_content: content of the post
    :return: positive, negative or neutral based on polarity
    """
    analysis = TextBlob(post_content)
    return 'positive' if analysis.sentiment.polarity > 0 else 'negative' if analysis.sentiment.polarity < 0 \
        else 'neutral'


def write_to_influx(crypto, sentiment, timestamp):
    """
    Writing sentiment data to influx db
    :param crypto: cryptocurrency name
    :param sentiment: positive, negative or neutral - result of sentiment analysis
    :param timestamp: timestamp of post
    :return: None
    """
    json_data = [
        {
            "measurement" : "crypto_sentiment",
            "tags": {
                "crypto": crypto,
            },
            "time": timestamp,
            "fields": {
                "sentiment": sentiment,
            }
        }
    ]
    influx_client.write_points(json_data)


for post in consumer:
    post_data = post.value
    crypto = post_data.get("crypto")
    title = post_data.get("title")
    crypto = post_data.get("crypto")
    content = post_data.get("content")
    timestamp = post_data.get("created_utc")

    # Perform sentiment analysis
    sentiment = analyse_sentiment(content)

    # Write Sentiment Data to Influx DB
    write_to_influx(crypto, sentiment, timestamp)

    time.sleep(1)


