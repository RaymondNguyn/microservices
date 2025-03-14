import connexion
from connexion import NoContent
import json
import functools
from datetime import datetime
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

with open('app_conf.yml', 'r') as f:
    app_conf = yaml.safe_load(f.read())


DATABASE_URL = f"mysql://{app_conf['datastore']['user']}:{app_conf['datastore']['password']}@{app_conf['datastore']['hostname']}:{app_conf['datastore']['port']}/{app_conf['datastore']['db']}"
engine = create_engine(DATABASE_URL)

with open("log_conf.yml", "r") as f:
    LOG_CONFIG = yaml.safe_load(f.read())
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

if __name__ == "__main__":
    setup_kafka_thread()
    Base.metadata.create_all(engine)
    app.run(port=8090, host="0.0.0.0")
