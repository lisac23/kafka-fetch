from kafka import KafkaProducer
import json
from config import logger
import atexit

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], 
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def publish_insight(insight, topic='insights'): 
    """
    Publishes an insight to the specified Kafka topic.

    Args:
        insight (dict): The insight to be published.
        topic (str): The Kafka topic to publish the insight to. Defaults to 'insights'.

    Returns:
        None
    """
    try: 
        producer.send(topic, value=insight)
        logger.info(f"Published insight: {insight}")
    except Exception as e: 
        logger.error(f"Failed to publish insight: {e}")


@atexit.register
def close_producer():
    """
    Closes the Kafka producer gracefully, ensuring all messages are flushed.

    Returns:
        None
    """
    try:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed.")
    except Exception as e:
        logger.warning(f"Error closing Kafka producer: {e}")