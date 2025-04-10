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
import asyncio

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

logger = logging.getLogger('basicLogger')

app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("consistency_check.yaml",base_path="/consist_check", strict_validation=True, validate_responses=True)

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

# /update
async def run_consistency_checks():
    processingResponse = await httpx.get(f"http://{base_url}/processing/stats")
    processingData = processingResponse.json()
    processingDict = {"wind":processingData["num_wind_readings"],"temp":processingData["num_temp_readings"]}






if __name__ == "__main__":
    app.run(port=8100, host="0.0.0.0")