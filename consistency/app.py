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


log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")
base_url = "nginx"

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("consistency_check.yaml", base_path="/consistency", strict_validation=True, validate_responses=True)

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

def compare_storage_analyzer():
    """Compare events between storage and analyzer services"""
    print("comparing")
    try:
        # Make synchronous requests with proper timeouts
        storage_wind_response = httpx.get(f"http://{base_url}/storage/listWind", timeout=30.0)
        analyzer_wind_response = httpx.get(f"http://{base_url}/analyzer/listWind", timeout=30.0)
        storage_temp_response = httpx.get(f"http://{base_url}/storage/listTemp", timeout=30.0)
        analyzer_temp_response = httpx.get(f"http://{base_url}/analyzer/listTemp", timeout=30.0)
        
        # Parse JSON responses
        storage_wind = storage_wind_response.json()
        analyzer_wind = analyzer_wind_response.json()
        storage_temp = storage_temp_response.json()
        analyzer_temp = analyzer_temp_response.json()

        # Extract all IDs and combine by source
        storage_ids = {event['trace_id'] for event in storage_wind["Wind_events"]}
        storage_ids.update({event['trace_id'] for event in storage_temp["Temp_events"]})
        
        analyzer_ids = {event['trace_id'] for event in analyzer_wind["Wind_events"]}
        analyzer_ids.update({event['trace_id'] for event in analyzer_temp["Temp_events"]})
        
        # Compare combined datasets
        in_analyzer_not_storage = list(analyzer_ids - storage_ids)
        in_storage_not_analyzer = list(storage_ids - analyzer_ids)
        
        return {
            "in_analyzer_not_storage": in_analyzer_not_storage,
            "in_storage_not_analyzer": in_storage_not_analyzer,
        }
    except Exception as e:
        logger.error(f"Error in compare_storage_analyzer: {str(e)}")
        return {
            "in_analyzer_not_storage": [],
            "in_storage_not_analyzer": [],
            "error": str(e)
        }

def check_processing():
    """Check processing service stats"""
    print("checking processing")
    try:
        processing_response = httpx.get(f"http://{base_url}/processing/stats", timeout=10.0)
        processing_data = processing_response.json()
        return {
            "wind_count": processing_data["num_wind_readings"],
            "temp_count": processing_data["num_temp_readings"]
        }
    except Exception as e:
        logger.error(f"Error checking processing: {str(e)}")
        return {"wind_count": 0, "temp_count": 0, "error": str(e)}

def check_storage():
    """Check storage service counts"""
    print("checking storage")
    try:
        storage_response = httpx.get(f"http://{base_url}/storage/count", timeout=10.0)
        return storage_response.json()
    except Exception as e:
        logger.error(f"Error checking storage: {str(e)}")
        return {"error": str(e)}

def check_analyzer():
    """Check analyzer service stats"""
    print("checking analyzer")
    try:
        analyzer_response = httpx.get(f"http://{base_url}/analyzer/stats", timeout=10.0)
        analyzer_data = analyzer_response.json()
        return {
            "wind_count": analyzer_data['num_wind'],
            "temp_count": analyzer_data['num_temp']
        }
    except Exception as e:
        logger.error(f"Error checking analyzer: {str(e)}")
        return {"wind_count": 0, "temp_count": 0, "error": str(e)}

def run_consistency_checks():
    """Run all consistency checks and save results"""
    print("running check")
    start_time = time.perf_counter()
    
    # Run all checks synchronously
    missing = compare_storage_analyzer()
    processing = check_processing()
    storage = check_storage()
    analyzer = check_analyzer()

    results = {
        "last_update": datetime.now(),
        "count": {
            "processing": processing,
            "database": storage,
            "queue": analyzer
        },
        "missing": missing
    }
    
    end_time = time.perf_counter()
    process_timer = int((end_time - start_time) * 1000)

    logger.info(f"Consistency check done | processing_time:{process_timer}ms | missing in storage: {len(missing['in_analyzer_not_storage'])} | missing in analyzer: {len(missing['in_storage_not_analyzer'])}")

    stats_file = "/app/data/data.json"
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(stats_file), exist_ok=True)

    with open(stats_file, 'w') as f:
        json.dump(results, f, default=str, indent=4)
    
    return results

def get_checks():
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
        default_stats = {"checks": [], "last_updated": None}
        
        with open(stats_file, 'w') as f:
            json.dump(default_stats, f)
        
        return default_stats
    except json.JSONDecodeError:
        # File exists but contains invalid JSON
        default_stats = {"checks": [], "last_updated": None}
        
        with open(stats_file, 'w') as f:
            json.dump(default_stats, f)
        
        return default_stats, 200

def init_scheduler():
    """Initialize the scheduler to run consistency checks periodically"""
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_consistency_checks, 'interval', seconds=300)  # Run every 5 minutes
    scheduler.start()
    logger.info("Scheduler started for consistency checks")

if __name__ == "__main__":
    init_scheduler()
    # Start the application
    app.run(port=9100, host="0.0.0.0")