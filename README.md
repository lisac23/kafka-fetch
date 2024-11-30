### kafka-fetch

## Overview

`kafka-fetch` is a take home project showcasing a Kafka-based data pipeline that processes user login events. It includes:
- A Kafka consumer (`kafka_consumer.py`) to fetch login events from the `user-login` topic.
- Insight generation functions to analyze and summarize the data (e.g., peak login hour, logins per region, device usage distribution).
- A Kafka producer (`kafka_insights_producer.py`) to publish the generated insights to the `insights` topic.

This project runs locally and uses Docker to set up the Kafka environment.

---

## Setup

### Prerequisites
- **Python**: Tested on Python 3.11.
- **Docker Desktop**: For running Kafka locally.

---

### 1. Create a Virtual Environment
To avoid interfering with system-wide Python packages, use a virtual environment. Run the following commands in the main directory of the repo:

```bash
python3 -m venv .venv
source .venv/bin/activate  # On Mac/Linux
# For Windows, use:
# .venv\Scripts\activate
```

---

### 2. Install Python Libraries
Install the required Python dependencies by running:

```bash
pip install -r requirements.txt
```

If your Python environment requires `pip3`:

```bash
pip3 install -r requirements.txt
```

---

### 3. Start Kafka Using Docker
Use Docker Compose to start a Kafka server locally. Run the following command from the main directory:

```bash
docker compose up
```

This will start the Kafka broker and Zookeeper as defined in the `docker-compose.yml` file.

---

### 4. Run the Kafka Consumer
To start processing login events, run the included Kafka consumer:

```bash
python3 kafka_consumer.py
```

The consumer will:
- Fetch messages from the `user-login` topic.
- Process the data in batches.
- Publish insights to the `insights` topic using the Kafka producer.

---

## Notes
- **kafka_insights_producer.py**: This script is imported and used by `kafka_consumer.py` to publish insights. While it can technically run independently, it is designed to be called from the consumer script.
- **Batch Size**: The default batch size for processing messages is set to 50. You can modify this value in `config.py`.

---

## Project Structure

```
kafka-fetch/
├── config.py                   # Shared configuration for batch size, logging, etc.
├── kafka_consumer.py           # Main consumer script
├── kafka_insights_producer.py  # Kafka producer for publishing insights
├── requirements.txt            # Python dependencies
├── docker-compose.yml          # Docker setup for Kafka
└── README.md                   # Project documentation
```

---

## Example Insights Generated

1. **Peak Login Hour**
   - Identifies the hour with the most login events.

2. **Logins Per Region**
   - Counts the number of logins grouped by region.

3. **Device Usage Distribution**
   - Calculates the percentage distribution of logins by device type.

---

## Troubleshooting

- **Kafka Not Starting**:
  Ensure Docker Desktop is running and the `docker compose up` command is executed from the correct directory containing the `docker-compose.yml` file.

- **Python Package Issues**:
  Verify that your virtual environment is activated before installing dependencies:
  ```bash
  source .venv/bin/activate
  ```

- **Corrupt Messages**:
  The consumer skips corrupt or incomplete messages and logs a warning. Check the logs for details.

---

## Future Enhancements
- Add support for real-time visualization of insights.
- Include integration with monitoring tools for metrics like pipeline health, lag, etc. 
- Set up a Dead Letter Queue for malformed messages. 
- Transition to using the confluent-kafka library which is more robust for production. 
- Add asynchronous processing of the data so it is non-blocking. 
- Add more advanced schema validation. 

---

## Making this application production ready!
- Deploy on AWS or other cloud that has a managed Kafka interface (or host directly). 
- Depending on size, add task queuing or parallel processing. 
- Employ backpressure parameters to optimize performance. 
- Potentially containerize the new consumer and producer for deployment. 