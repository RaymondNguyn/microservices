import connexion
from connexion import NoContent
import json
from threading import Lock
import yaml
import logging
import logging.config
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from flask import jsonify
import time
import os
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
from kafka_wrapper import KafkaWrapper 

config_file_path = os.getenv("CONFIG_FILE")
log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")
base_url = os.getenv("BASE_URL")

# Set up config  files
with open(config_file_path, 'r') as f:
    app_config = yaml.safe_load(f.read())

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)



# logger set up
logger = logging.getLogger('basicLogger')

# Flask app set up
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml",base_path="/analyzer", strict_validation=True, validate_responses=True)

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=[
        "http://localhost",
        "http://localhost:80",
        f"http://${base_url}:80"
        ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# api
def getWindSpeedEvent(index):
    kafka = KafkaWrapper(f"{app_config['events']['hostname']}:{app_config['events']['port']}", 
                         str.encode(app_config["events"]["topic"]))
    
    counter = 0
    for msg in kafka.messages():
        message = msg.value.decode("utf-8")
        data = json.loads(message)
        payload = data["payload"]

        if data.get("type") == "wind-speed":
            if counter == index:
                logger.info(f"Found wind-speed event at index {index}")
                return payload, 200
            counter += 1

    logger.warning(f"No wind-speed event found at index {index}")
    return {"message": f"No message at index {index}!"}, 404

    

def getTemperatureEvent(index):
    kafka = KafkaWrapper(f"{app_config['events']['hostname']}:{app_config['events']['port']}", 
                         str.encode(app_config["events"]["topic"]))
    
    counter = 0
    for msg in kafka.messages():
        message = msg.value.decode("utf-8")
        data = json.loads(message)
        payload = data["payload"]
        logger.info(f"{payload}")

        if data.get("type") == "temperature":
            if counter == index:
                logger.info(f"Found temp event at index {index}")
                return payload, 200
            counter += 1

    logger.warning(f"No temp event found at index {index}")
    return {"message": f"No message at index {index}!"}, 404

def getStats():
    hostname = f"{app_config["events"]["hostname"]}:{app_config["events"]["port"]}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(f"{app_config["events"]["topic"]}")]

    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=True,
        consumer_timeout_ms=1000
    )
    
    stats = {
        "num_wind":0,
        "num_temp":0
    }

    for msg in consumer:
        message = msg.value.decode("utf-8")
        data = json.loads(message)

        event_type = data.get("type")
        if event_type == "wind-speed":
            stats["num_wind"] += 1
        elif event_type == "temperature":
            stats["num_temp"] += 1

    return stats, 201

### Assignment2
def get_wind_event_id(limit=100):
    """
    Get a list of wind speed event IDs and trace IDs from Kafka
    With a limit parameter and timeout to avoid freezing
    """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    # Use consumer_timeout_ms to prevent indefinite blocking
    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=True,
        consumer_timeout_ms=5000  # 5 second timeout
    )
    
    wind_events = []
    msg_count = 0
    
    logger.info(f"Fetching wind speed events (limited to {limit})")
    
    try:
        for msg in consumer:
            if msg is None:
                break
                
            # Safety limit on messages processed
            msg_count += 1
            if msg_count > 10000:
                logger.warning("Reached maximum message scan limit")
                break
                
            # Stop when we've collected enough events
            if len(wind_events) >= limit:
                break
                
            message = msg.value.decode("utf-8")
            data = json.loads(message)
            
            # Only collect wind speed events
            if data.get("type") == "wind-speed":
                payload = data["payload"]
                wind_events.append({
                    "event_id": payload.get("eventID"),
                    "trace_id": payload.get("traceID")
                })
                
        logger.info(f"Found {len(wind_events)} wind speed events")
        response = {"Wind_events":wind_events}
        return response, 200
        
    except Exception as e:
        logger.error(f"Error retrieving wind speed events: {str(e)}")
        return {"message": f"Error retrieving events: {str(e)}"}, 500
    finally:
        # Always close the consumer to prevent resource leaks
        consumer.stop()


def get_temp_event_id(limit=100):
    """
    Get a list of temperature event IDs and trace IDs from Kafka
    With a limit parameter and timeout to avoid freezing
    """
    hostname = f"{app_config['events']['hostname']}:{app_config['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    
    # Use consumer_timeout_ms to prevent indefinite blocking
    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=True,
        consumer_timeout_ms=5000  # 5 second timeout
    )
    
    temperature_events = []
    msg_count = 0
    
    logger.info(f"Fetching temperature events (limited to {limit})")
    
    try:
        for msg in consumer:
            if msg is None:
                break
                
            # Safety limit on messages processed
            msg_count += 1
            if msg_count > 10000:
                logger.warning("Reached maximum message scan limit")
                break
                
            # Stop when we've collected enough events
            if len(temperature_events) >= limit:
                break
                
            message = msg.value.decode("utf-8")
            data = json.loads(message)
            
            # Only collect temperature events
            if data.get("type") == "temperature":
                payload = data["payload"]
                temperature_events.append({
                    "event_id": payload.get("eventID"),
                    "trace_id": payload.get("traceID")
                })
                
        logger.info(f"Found {len(temperature_events)} temperature events")
        response = {"Temp_events":temperature_events}
        return response, 200
        
    except Exception as e:
        logger.error(f"Error retrieving temperature events: {str(e)}")
        return {"message": f"Error retrieving events: {str(e)}"}, 500
    finally:
        # Always close the consumer to prevent resource leaks
        consumer.stop()

if __name__ == "__main__":
    logger.info("Logger is working!")
    # setup_kafka_thread()
    app.run(port=8900, host="0.0.0.0")
