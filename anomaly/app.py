import connexion
from connexion import NoContent
import json
import time
from datetime import datetime, timezone, timedelta
import logging
import logging.config
import yaml
import os
from apscheduler.schedulers.background import BackgroundScheduler
import httpx
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware
import asyncio
from kafka_wrapper import KafkaWrapper 

config_file_path = os.getenv("CONFIG_FILE")
log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")
WIND_THRESH = os.getenv("ANON_WIND")
TEMP_THRESH = os.getenv("ANON_TEMP")
base_url = os.getenv("BASE_URL")

with open(config_file_path, 'r') as f:
    app_config = yaml.safe_load(f.read())

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("anomaly.yaml", base_path="/anomaly", strict_validation=True, validate_responses=True)

app.add_middleware(
    CORSMiddleware,
    position=MiddlewarePosition.BEFORE_EXCEPTION,
    allow_origins=[
        "http://localhost",
        "http://localhost:80",
        f"http://{base_url}:80"
        ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def update_anomalies():
    logger.debug("Updating anomalies")
    stats_file = "/app/data/data.json"
    os.makedirs(os.path.dirname(stats_file), exist_ok=True)
    kafka = KafkaWrapper(f"{app_config['events']['hostname']}:{app_config['events']['port']}", 
                         str.encode(app_config["events"]["topic"]))
    
    results = []
    for msg in kafka.messages():
        message = msg.value.decode("utf-8")
        data = json.loads(message)
        payload = data["payload"]

        if data.get("type") == "wind-speed":
            if data.get("windspeed") <= WIND_THRESH:
                logger.debug("Anomaly found at event wind")
                results.append({
                    "eventID": payload["eventID"],
                    "traceID": payload["traceID"],
                    "type": "wind",
                    "Threshold": WIND_THRESH - int(payload["windspeed"])
                })


        if data.get("type") == "temp":
            if data.get("temperature") <= TEMP_THRESH:
                logger.debug("Anomaly found at event wind")
                results.append({
                    "eventID": payload["eventID"],
                    "traceID": payload["traceID"],
                    "type": "temp",
                    "Threshold": TEMP_THRESH - int(payload["temperature"])
                })
    
    print(results)
    with open(stats_file, 'w') as f:
        json.dump(results, f, default=str, indent=4)

    return {"hello"}, 200


def get_anomalies():
    """Get the current consistency check results"""
    stats_file = "/app/data/data.json"
    
    # Check if directory exists, create if not
    os.makedirs(os.path.dirname(stats_file), exist_ok=True)
    
    try:
        with open(stats_file, 'r') as f:
            stats = json.load(f)
            if stats == "":
                return {"message": "No data found"}, 404
            else:
                return stats
    except FileNotFoundError:
        # File doesn't exist, create it with default content
        default_stats = [{
            "event_id": "",
            "trace_id": "",
            "event_type": "",
            "anomaly_type": "",
            "description": ""
        }]
        
        with open(stats_file, 'w') as f:
            json.dump(default_stats, f)
        
        return default_stats
    except json.JSONDecodeError:
        # File exists but contains invalid JSON
        default_stats = [{
            "event_id": "",
            "trace_id": "",
            "event_type": "",
            "anomaly_type": "",
            "description": ""
        }]
        
        with open(stats_file, 'w') as f:
            json.dump(default_stats, f)
        
        return default_stats, 200




if __name__ == "__main__":
    # Start the application
    logger.info(f"Threshold of anomaly Wind {WIND_THRESH} and threshhold of temp is {TEMP_THRESH}")
    app.run(port=9200, host="0.0.0.0")