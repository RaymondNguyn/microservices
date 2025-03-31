import connexion
from connexion import NoContent
import json
from datetime import datetime, timezone, timedelta
import logging
import logging.config
import yaml
import os
from apscheduler.schedulers.background import BackgroundScheduler
import httpx
from connexion.middleware import MiddlewarePosition
from starlette.middleware.cors import CORSMiddleware

config_file_path = os.getenv("CONFIG_FILE")
log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")
base_url = os.getenv("BASE_URL")


with open(config_file_path, 'r') as f:
    Conf = yaml.safe_load(f.read())
    print(Conf)

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)

TEMP_URL = Conf["events"]["temperature"]["url"]
WIND_URL = Conf["events"]["wind"]["url"]

logger = logging.getLogger('basicLogger')

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml",base_path="/processing", strict_validation=True, validate_responses=True)

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



def populate_stats():
    logger.info("Processing has started")

    stats_file = "/app/data/data.json"

    # Default stats in case the file is missing or invalid
    default_stats = {
        "num_temp_readings": 0,
        "max_temp_readings": 0,
        "num_wind_readings": 0,
        "max_wind_readings": 0,
        "last_updated": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()  # Fixed missing parenthesis
    }

    if os.path.exists(stats_file):
        try:
            with open(stats_file, 'r') as f:
                stats = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            logger.warning("Error reading stats file. Using default stats.")
            stats = default_stats
    else:
        logger.info("Stats file does not exist. Using default stats.")
        stats = default_stats
    
    last_updated = stats["last_updated"]
    current_time = datetime.now(timezone.utc).isoformat()
    logger.debug(f"Current time: {current_time}")

    if last_updated != current_time:
        logger.info(f"Updating last updated timestamp to: {current_time}")
        stats["last_updated"] = current_time
    
    # Debug the request parameters
    logger.debug(f"Querying with parameters: start_timestamp={last_updated}, end_timestamp={current_time}")
    
    start_time = datetime.fromisoformat(last_updated) - timedelta(minutes=1)
    params = {"start_timestamp": start_time.isoformat(), "end_timestamp": current_time}

    try:
        wind_response = httpx.get(WIND_URL, params=params)
        temp_response = httpx.get(TEMP_URL, params=params)
        
        # Debug the responses
        logger.debug(f"Wind response: {wind_response.status_code}")
        logger.debug(f"Temp response: {temp_response.status_code}")
        
        wind_events = wind_response.json() if wind_response.status_code == 200 else []
        temp_events = temp_response.json() if temp_response.status_code == 200 else []  # Fixed variable name from event to events
        if wind_response.status_code != 200:
            logger.error(f"Wind API error: {wind_response.text}")
        if temp_response.status_code != 200:
            logger.error(f"Temp API error: {temp_response.text}")
        
        logger.debug(f"Wind events: {len(wind_events)}")
        logger.debug(f"Temp events: {len(temp_events)}")

        if not temp_events and not wind_events:
            logger.error("Failed to get events or no new events")
        
        if wind_events:
            try:
                wind_values = [event["windspeed"] for event in wind_events]  # Fixed field name to "windspeed"
                stats["num_wind_readings"] += len(wind_values)
                stats["max_wind_readings"] = max(stats["max_wind_readings"], max(wind_values))
                logger.info(f"Processed {len(wind_values)} wind readings")
            except KeyError as e:
                logger.error(f"Key error in wind events: {e}")
                logger.debug(f"Wind event data: {wind_events}")
        
        if temp_events:
            try:
                temp_values = [event["temperature"] for event in temp_events]
                stats["num_temp_readings"] += len(temp_values)
                stats["max_temp_readings"] = max(stats["max_temp_readings"], max(temp_values))
                logger.info(f"Processed {len(temp_values)} temperature readings")
            except KeyError as e:
                logger.error(f"Key error in temperature events: {e}")
                logger.debug(f"Temperature event data: {temp_events}")
                
        
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=4)
            
    except Exception as e:
        logger.error(f"Error during processing: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    
    logger.info("Processing has ended")


def get_stats():
    stats_file = "/app/data/data.json"
    if os.path.exists(stats_file) and os.stat(stats_file).st_size > 0:
        try:
            with open(stats_file, "r") as f:
                stats = json.load(f)
            return stats, 200
        except json.JSONDecodeError:
            logger.error("Invalid JSON in stats file")
            return {"message": "Statistics data not available"}, 500
    else:
        logger.error("Stats file not found or empty")
        return {"message": "Statistics not available"}, 404

def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(
        populate_stats,
        'interval',
        seconds=Conf['scheduler']['interval']
    )
    sched.start()


if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, host="0.0.0.0")