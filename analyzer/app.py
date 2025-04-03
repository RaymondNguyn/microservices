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
    

if __name__ == "__main__":
    logger.info("Logger is working!")
    # setup_kafka_thread()
    app.run(port=8900, host="0.0.0.0")
