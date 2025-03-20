import connexion
from connexion import NoContent
import json
from datetime import datetime
import logging
import logging.config
import yaml
import os
from apscheduler.schedulers.background import BackgroundScheduler
import httpx

config_file_path = os.getenv("CONFIG_FILE")
log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")

with open(config_file_path, 'r') as f:
    Conf = yaml.safe_load(f.read())
    print(Conf)

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)

TEMP_URL=Conf["events"]["temperature"]["url"]
WIND_URL=Conf["events"]["wind"]["url"]


logger = logging.getLogger('basicLogger')


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

def populate_stats():
    logger.info("Processing has started")
    print("In function populate stats!")

    stats_file = "/app/data/data.json"

    # Default stats in case the file is missing or invalid
    default_stats = {
        "num_temp_readings": 0,
        "max_temp_readings": 0,
        "num_wind_readings": 0,
        "max_wind_readings": 0,
        "last_updated": datetime.now().isoformat()  # Changed from datetime.min to avoid potential issues
    }

    # Read stats file safely
    try:
        if os.path.exists(stats_file) and os.stat(stats_file).st_size > 0:
            with open(stats_file, 'r') as f:
                stats = json.load(f)
        else:
            logger.warning(f"Stats file missing or empty. Using default stats.")
            stats = default_stats
            # Immediately write the default stats to file
            with open(stats_file, "w") as f:
                json.dump(stats, f, indent=4)
    except json.JSONDecodeError as e:
        logger.error(f"Corrupt JSON in {stats_file}. Resetting to default stats. Error: {e}")
        stats = default_stats
        # Immediately write the default stats to file
        with open(stats_file, "w") as f:
            json.dump(stats, f, indent=4)
    except Exception as e:
        logger.error(f"Unexpected error reading {stats_file}: {e}")
        stats = default_stats
        # Immediately write the default stats to file
        with open(stats_file, "w") as f:
            json.dump(stats, f, indent=4)

    last_updated = stats.get("last_updated", datetime.now().isoformat())
    current_time = datetime.now().isoformat()

    # Rest of the function continues as before...
    temp_url = f"{TEMP_URL}?start_timestamp={last_updated}&end_timestamp={current_time}"
    wind_url = f"{WIND_URL}?start_timestamp={last_updated}&end_timestamp={current_time}"

    temp_response = httpx.get(temp_url)
    wind_response = httpx.get(wind_url)

    if temp_response.status_code == 200:
        temp_events = temp_response.json()
        num_temp_readings = len(temp_events)
        max_temp = max((event["temperature"] for event in temp_events), default=stats["max_temp_readings"])
    else:
        logger.error(f"Failed to get temperature events. Status Code: {temp_response.status_code}")
        num_temp_readings = 0
        max_temp = stats["max_temp_readings"]

    if wind_response.status_code == 200:
        wind_events = wind_response.json()
        num_wind_readings = len(wind_events)
        max_wind = max((event["windspeed"] for event in wind_events), default=stats["max_wind_readings"])
    else:
        logger.error(f"Failed to get wind events. Status Code: {wind_response.status_code}")
        num_wind_readings = 0
        max_wind = stats["max_wind_readings"]

    stats["num_temp_readings"] += num_temp_readings
    stats["num_wind_readings"] += num_wind_readings
    stats["max_temp_readings"] = max(stats["max_temp_readings"], max_temp)
    stats["max_wind_readings"] = max(stats["max_wind_readings"], max_wind)
    stats["last_updated"] = current_time

    # Write updated stats safely
    try:
        with open(stats_file, "w") as f:
            json.dump(stats, f, indent=4)
    except IOError as e:
        logger.error(f"Failed to write to {stats_file}: {e}")

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
