# kafka-fetch

## SETUP

# Prerequisites: 
 - Python installed (this was tested on Python version 11)
 - Docker Desktop installed and running 

# Create your venv
It's safest to run Python in a venv so it doesn't interfere with the system installs. Instructions should work on Mac or Linux (tested on Linux Mint). 
- Run commands in the main directory of the repo. 
  bash```python3 -m venv .venv```
  bash```source .venv/source/activate```

# Install Python libraries
- Run command depending on which version of pip is available:
  bash```pip install -r requirements.txt```
  OR 
  bash```pip3 install -r requirments.txt```

# Running the Kafka producer from Docker
- Run command from the main folder: 
  bash```docker compose up``` 

# To run the included consumer: 
- Run command from the main folder: 
  bash```python3 kafka_consumer.py```

# Note that the kafka_insights_producer.py script is called from the kafka_consumer.py script.  kafka_insights_producer.py will run independently but it's not meant to. The only script to execute is kafka_consumer.py
