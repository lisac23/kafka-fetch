from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], 
    value_serializer=lambda x: json.dumps(x).encode('utf-8'))


def publish_insight(insight, topic='insights'): 
    try: 
        producer.send(topic, value=insight)
        producer.flush()
        print(f"Published insight: {insight}")
    except Exception as e: 
        print(f"Failed to publish insight: {e}")

