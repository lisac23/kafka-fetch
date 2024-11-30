import pandas as pd
from collections import defaultdict
from kafka import KafkaConsumer
from kafka_insights_producer import publish_insight
import json
from config import logger, BATCH_SIZE

consumer = KafkaConsumer(
    'user-login', 
    bootstrap_servers=['localhost:29092'], 
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    group_id='my_python_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logger.info("Consumer is listening to the 'user-login' topic...")

data = []

hourly_counts = defaultdict(int)

def update_peak_hour(df):
    """
    Calculate and update the peak login hour.

    Args:
        df (pd.DataFrame): Batch of login events with a 'timestamp' column.

    Returns:
        dict: Insight with the peak hour and corresponding login count.
    """
    df['hour'] = pd.to_datetime(df['timestamp'], unit='s').dt.hour

    batch_counts = df['hour'].value_counts()

    for hour, count in batch_counts.items(): 
        hourly_counts[hour] += count

    peak_hour = max(hourly_counts, key=hourly_counts.get)

    return {"metric": "peak_hour", "hour": peak_hour, "logins": hourly_counts[peak_hour]}

def calculate_logins_per_region(df):
    """
    Calculates the number of logins per region based on the current batch.

    Args:
        df (pd.DataFrame): DataFrame containing login events with a 'locale' column.

    Returns:
        list: A list of insights, each containing the region and corresponding login count.
    """
    region_counts = df['locale'].value_counts().reset_index()
    region_counts.columns = ['region', 'count']
    insights = [{"metric": "logins_per_region", "region": row['region'], "count": row['count']}
                for _,row in region_counts.iterrows()]

    return insights

def calculate_device_usage(df):
    """
    Computes the percentage distribution of logins by device type.

    Args:
        df (pd.DataFrame): DataFrame containing login events with a 'device_type' column.

    Returns:
        list: A list of insights with device type and percentage usage.
    """
    device_distribution = df['device_type'].value_counts(normalize=True) * 100
    insights = [{"metric": "device_usage", "device_type": device, "percentage": round(percentage, 2)}
                 for device, percentage in device_distribution.items()]

    return insights

def process_batch(data):
    """
    Processes a batch of login events to generate insights.

    Args:
        data (list): A batch of login events in dictionary format.

    Returns:
        list: A list of insights derived from the batch.
    """
    if not data:
        logger.info("No valid messages to process in this batch.")
        return []
    
    logger.info(f"Processing batch with {len(data)} messages.")
    df = pd.DataFrame(data)

    insights = []
    insights.append(update_peak_hour(df))
    insights.extend(calculate_logins_per_region(df))
    insights.extend(calculate_device_usage(df))

    return insights

def safe_publish(insight):
    """
    Publishes an insight to the Kafka topic with error handling.

    Args:
        insight (dict): The insight to be published.

    Returns:
        None
    """
    try:
        publish_insight(insight)
    except Exception as e:
        logger.warning(f"Failed to publish insight: {insight}, error: {e}")

def is_valid_record(record):
    """
    Validates a record to ensure it contains all required fields.

    Args:
        record (dict): A login event record.

    Returns:
        bool: True if the record is valid, False otherwise.
    """
    required_fields = ['user_id', 'app_version', 'ip', 'timestamp', 'locale', 'device_type', 'device_id']
    return all(field in record for field in required_fields)

try: 
    for message in consumer:
        record = message.value
        logger.info(f"Consumed message: {record}")
        # validation step to ensure all fields are present
        if is_valid_record(record):
            data.append(record)
        else:
           logger.warning(f"Skipping corrupt message: {record}")

        if len(data)>= BATCH_SIZE: 
            insights = process_batch(data)

            for insight in insights: 
                safe_publish(insight)

            data = []
except Exception as e:
    logger.error(f"Error processing messages: {e}")
finally:
    consumer.close()
    logger.info("Consumer has been shut down.")