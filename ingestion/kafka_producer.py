from kafka import KafkaProducer
from integration.api.config import kafka_bootstrap_servers
import json


def create_producer(bootstrap_servers):
    """
    Creates Kafka Producer with given bootstrap servers and value serializer

    Parameters:
        bootstrap_servers (list): list of bootstrap servers

    Returns:
        KafkaProducer: A kafka producer instance
    """
    return KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))


def send_message(producer, topic, message):
    """
    Producer sends the message to the Kafka topic

    Parameters:
        producer (KafkaProducer): The Kafka Producer instance
        topic (str): The Kafka topic to which the message is sent
        message (dict): The message to be sent

    Returns:
        None
    """
    producer.send(topic, message)
    producer.flush()


if __name__ == '__main__':
    kafka_producer = create_producer(kafka_bootstrap_servers)
    send_message(kafka_producer, "test_topic", {"test1": "123"})
