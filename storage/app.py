import connexion
from connexion import NoContent
import json
import functools
from datetime import datetime, timezone, timedelta
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
from flask import jsonify  # Missing import

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
    hostname = f"{app_conf['events']['hostname']}:{app_conf['events']['port']}"
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_conf["events"]["topic"])]  # Fixed string formatting

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
            store_wind_event(payload, trace_id)
            
        elif msg.get("type") == "temperature":
            store_temperature_event(payload, trace_id)

        consumer.commit_offsets()

def parse_timestamp(timestamp_str):
    """
    Consistently parse ISO 8601 timestamps with either Z or +00:00 timezone markers
    """
    # Replace Z with +00:00 to standardize format
    if timestamp_str.endswith('Z'):
        timestamp_str = timestamp_str[:-1] + '+00:00'
    
    # Ensure T separator
    if ' ' in timestamp_str and 'T' not in timestamp_str:
        timestamp_str = timestamp_str.replace(' ', 'T', 1)
    
    try:
        return datetime.fromisoformat(timestamp_str)
    except ValueError as e:
        logger.error(f"Failed to parse timestamp '{timestamp_str}': {e}")
        # Fall back to current time minus 5 minutes if parsing fails
        return datetime.now(timezone.utc) - timedelta(minutes=5)


@use_db_session
def store_wind_event(session, payload, trace_id):
    location_str = json.dumps(payload["location"])
    report_wind_speed = WindReport(
        event_id=payload["eventID"],
        device_id=payload["deviceID"],
        timeStamp=parse_timestamp(payload["timeStamp"]),
        windspeed=payload["windspeed"],
        location=location_str,
        trace_id=trace_id
    )
    session.add(report_wind_speed)
    session.commit()
    return NoContent, 201

@use_db_session
def store_temperature_event(session, payload, trace_id):
    report_temp = TempReport(
        event_id=payload["eventID"],
        device_id=payload["deviceID"],
        timeStamp=parse_timestamp(payload["timeStamp"]),
        temperature=payload["temperature"],
        trace_id=trace_id
    )
    session.add(report_temp)
    session.commit()
    return NoContent, 201

@use_db_session
def get_windspeed_readings(session, start_timestamp, end_timestamp):
    try:
        # Consider using a wider time window or checking event creation time
        # Option 1: Add a margin to account for older event timestamps
        start_time = parse_timestamp(start_timestamp) - timedelta(minutes=15)
        end_time = parse_timestamp(end_timestamp)
        
        logger.debug(f"Querying temp events from {start_time}, {end_time}")
        
        # Change to query on timeStamp instead of date_created
        statement = select(WindReport).where(
            WindReport.timeStamp >= start_time,
            WindReport.timeStamp < end_time
        )
        
        events = session.execute(statement).scalars().all()
        logger.info(f"Found {len(events)} temp events")
        
        return jsonify([
            {
                "eventID": event.event_id,  # Add this line
                "trace_id": event.trace_id,
                "deviceID": event.device_id,
                "timeStamp": event.timeStamp.isoformat(),
                "windspeed": event.windspeed,
                "location": event.location

            }
            for event in events
        ]), 200
    except Exception as e:
        logger.error(f"Error in get_wind_readings: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": "Internal server error"}), 500

@use_db_session
def get_temp_readings(session, start_timestamp, end_timestamp):
    try:
        # Consider using a wider time window or checking event creation time
        # Option 1: Add a margin to account for older event timestamps
        start_time = parse_timestamp(start_timestamp) - timedelta(minutes=15)
        end_time = parse_timestamp(end_timestamp)
        
        logger.debug(f"Querying temp events from {start_time}, {end_time}")
        
        # Change to query on timeStamp instead of date_created
        statement = select(TempReport).where(
            TempReport.timeStamp >= start_time,
            TempReport.timeStamp < end_time
        )
        
        events = session.execute(statement).scalars().all()
        logger.info(f"Found {len(events)} temp events")
        
        return jsonify([
            {
                "eventID": event.event_id,  # Add this line
                "trace_id": event.trace_id,
                "deviceID": event.device_id,
                "temperature": event.temperature,
                "timeStamp": event.timeStamp.isoformat()
            }
            for event in events
        ]), 200
    except Exception as e:
        logger.error(f"Error in get_temp_readings: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({"error": "Internal server error"}), 500

if __name__ == "__main__":
    setup_kafka_thread()
    Base.metadata.create_all(engine)
    app.run(port=8090, host="0.0.0.0")