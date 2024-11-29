import pandas as pd
from collections import defaultdict
from kafka import KafkaConsumer
from kafka_insights_producer import publish_insight
import json

#configurable batch size
BATCH_SIZE = 100

consumer = KafkaConsumer(
    'user-login', 
    bootstrap_servers=['localhost:29092'], 
    auto_offset_reset='earliest', 
    enable_auto_commit=True,
    group_id='my_python_consumer_group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Consumer is listening to the 'user-login' topic...")

data = []

hourly_counts = defaultdict(int)

def update_peak_hour(df):
    df['hour'] = pd.to_datetime(df['timestamp'], unit='s').dt.hour

    batch_counts = df['hour'].value_counts()
    print(batch_counts)

    for hour, count in batch_counts.items(): 
        hourly_counts[hour] += count

    peak_hour = max(hourly_counts, key=hourly_counts.get)
    print(f"Peak hour: {peak_hour}, logins: {hourly_counts[peak_hour]}")
    return {"metric": "peak_hour", "hour": peak_hour, "logins": hourly_counts[peak_hour]}

def calculate_logins_per_region(df):
    region_counts = df['locale'].value_counts().reset_index()
    region_counts.columns = ['region', 'count']
    insights = [{"metric": "logins_per_region", "region": row['region'], "count": row['count']}
                for _,row in region_counts.iterrows()]
    print(insights)
    return insights

def calculate_device_usage(df):
    device_distribution = df['device_type'].value_counts(normalize=True) * 100
    insights = [{"metric": "device_usage", "device_type": device, "percentage": round(percentage, 2)}
                 for device, percentage in device_distribution.items()]
    print(insights)
    return insights

try: 
    for message in consumer:
        record = message.value
        print(f"Consumed message: {record}")
        data.append(record)

        if len(data)>= BATCH_SIZE: 
            print("Processing batch of messages...")
            df = pd.DataFrame(data)

            insights = []
            insights.append(update_peak_hour(df))
            insights.extend(calculate_logins_per_region(df))
            insights.extend(calculate_device_usage(df))

            for insight in insights: 
                publish_insight(insight)

            data = []
except KeyboardInterrupt: 
    print("Consumer stopped by user.")
finally: 
    consumer.close()