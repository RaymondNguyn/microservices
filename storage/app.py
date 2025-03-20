import connexion
from connexion import NoContent
import json
import functools
from datetime import datetime, timezone
from threading import Lock
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, Session
from model import Base, WindReport, TempReport
import logging
import logging.config
import yaml
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import uuid
import os

config_file_path = os.getenv("CONFIG_FILE")
log_config_path = os.getenv("LOG_CONFIG_FILE")
log_file_path = os.getenv("LOG_FILE")


with open(config_file_path, 'r') as f:
    app_conf = yaml.safe_load(f.read())


DATABASE_URL = f"mysql://{app_conf['datastore']['user']}:{app_conf['datastore']['password']}@{app_conf['datastore']['hostname']}:{app_conf['datastore']['port']}/{app_conf['datastore']['db']}"
engine = create_engine(DATABASE_URL)

with open(log_config_path, "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
    LOG_CONFIG['handlers']['file']['filename'] = log_file_path
    logging.config.dictConfig(LOG_CONFIG)

logger = logging.getLogger('basicLogger')


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

# threading set up
def setup_kafka_thread():
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()

def make_session():
    return sessionmaker(bind=engine)()

def use_db_session(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        session = make_session()
        try:
            return func(session, *args, **kwargs)
        finally:
            session.close()
    return wrapper

def process_messages():
    hostname = f"{app_conf["events"]["hostname"]}:{app_conf["events"]["port"]}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(f"{app_conf["events"]["topic"]}")]

    consumer = topic.get_simple_consumer(
        consumer_group=b'event_group',
        reset_offset_on_start=False,
        auto_offset_reset=OffsetType.LATEST
    )

    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s", msg)
        trace_id = str(uuid.uuid4())
        payload = msg["payload"]
        
        if msg.get("type") == "wind-speed":
            store_wind_event(payload,trace_id)
            
        elif msg.get("type") == "temperature":
            store_temperature_event(payload,trace_id)

        consumer.commit_offsets()

@use_db_session
def store_wind_event(session,payload,trace_id):

    try:
        timestamp = datetime.strptime(payload["timeStamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        timestamp = datetime.strptime(payload["timeStamp"], "%Y-%m-%dT%H:%M:%SZ")

    location_str = json.dumps(payload["location"])
    report_wind_speed = WindReport(
        event_id=payload["eventID"],
        device_id=payload["deviceID"],
        timeStamp=timestamp,
        windspeed=payload["windspeed"],
        location=location_str,
        trace_id=trace_id
    )
    session.add(report_wind_speed)
    session.commit()
    return NoContent,201

@use_db_session
def store_temperature_event(session,payload, trace_id):

    try:
        timestamp = datetime.strptime(payload["timeStamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        timestamp = datetime.strptime(payload["timeStamp"], "%Y-%m-%dT%H:%M:%SZ")

    report_temp = TempReport(
        event_id=payload["eventID"],
        device_id=payload["deviceID"],
        timeStamp=timestamp,
        temperature=payload["temperature"],
        trace_id=trace_id
    )
    session.add(report_temp)
    session.commit()
    return NoContent,201

def parse_iso_timestamp(timestamp_str):
    """Parse a timestamp string in ISO format to a datetime object with UTC timezone"""
    # Remove trailing Z if present
    timestamp_str = timestamp_str.rstrip('Z')
    
    # Try to handle the problematic +00:00 pattern
    if '+' in timestamp_str:
        # Split at the plus sign and just keep the date/time part
        timestamp_str = timestamp_str.split('+')[0]
    
    # Handle any remaining timezone info
    if ' ' in timestamp_str:
        # If there's a space (which could be from a replaced '+'), remove everything after it
        timestamp_str = timestamp_str.split(' ')[0]
    
    # Parse the clean timestamp and ensure UTC timezone
    dt = datetime.fromisoformat(timestamp_str)
    return dt.replace(tzinfo=timezone.utc)

# Processing endpoints
def get_windspeed_readings(start_timestamp, end_timestamp):
    """Gets wind speed readings between the start and end timestamps"""
    session = make_session()
    try:
        start = parse_iso_timestamp(start_timestamp)
        end = parse_iso_timestamp(end_timestamp)
        
        statement = select(WindReport).where(
            WindReport.timeStamp >= start,
            WindReport.timeStamp < end
        )
        
        results = [
            {
                "eventID": result.event_id,
                "deviceID": result.device_id,
                "timeStamp": result.timeStamp.isoformat() + 'Z',
                "windspeed": result.windspeed,
                "location": json.loads(result.location),
                "trace_id": result.trace_id
            }
            for result in session.execute(statement).scalars().all()
        ]
        
        logger.info("Found %d wind events (start: %s, end: %s)", len(results), start, end)
        return results
    finally:
        session.close()

def get_temp_readings(start_timestamp, end_timestamp):
    """Gets temperature readings between the start and end timestamps"""
    session = make_session()
    try:
        start = parse_iso_timestamp(start_timestamp)
        end = parse_iso_timestamp(end_timestamp)
        
        statement = select(TempReport).where(
            TempReport.timeStamp >= start,
            TempReport.timeStamp < end
        )
        
        results = [
            {
                "eventID": result.event_id,
                "deviceID": result.device_id,
                "timeStamp": result.timeStamp.isoformat() + 'Z',
                "temperature": result.temperature,
                "trace_id": result.trace_id
            }
            for result in session.execute(statement).scalars().all()
        ]
        
        logger.info("Found %d temperature events (start: %s, end: %s)", len(results), start, end)
        return results
    finally:
        session.close()



if __name__ == "__main__":
    setup_kafka_thread()
    Base.metadata.create_all(engine)
    app.run(port=8090, host="0.0.0.0")
