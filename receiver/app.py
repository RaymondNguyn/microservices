import connexion
from connexion import NoContent
import json
import os
from datetime import datetime
from threading import Lock
import uuid
import yaml
import logging
import logging.config
from pykafka import KafkaClient

# Set up config  files
config_file_path = os.getenv("CONFIG_FILE")
log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")

with open(config_file_path, 'r') as f:
    app_config = yaml.safe_load(f.read())

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)

# Kafka Set up
client = KafkaClient(hosts=f"{app_config["events"]["hostname"]}:{app_config["events"]["port"]}")
topic = client.topics[str.encode(app_config["events"]["topic"])]
producer = topic.get_sync_producer()

# logger set up
logger = logging.getLogger('basicLogger')

# Flask app set up
app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

# api
def createWindSpeedEvent(body):
    msg = {
        "type":"wind-speed",
        "datetime":datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload":body
    }
    
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    print(msg_str)
    return NoContent,201

def createTemperatureEvent(body):
    msg = {
        "type":"temperature",
        "datetime":datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload":body
    }
    
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    print(msg_str)
    return NoContent,201

if __name__ == "__main__":
    logger.info("Logger is working!")
    app.run(port=8080, host="0.0.0.0")
