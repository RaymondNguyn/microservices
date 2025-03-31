import time
import random
import json
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
from pykafka.common import OffsetType
import logging

logger = logging.getLogger('basicLogger')

class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.consumer = None
        self.connect()

    def connect(self):
        """ Infinite loop to keep trying to connect to Kafka """
        while True:
            if self.make_client() and self.make_consumer():
                break
            time.sleep(random.randint(500, 1500) / 1000)  # Sleep for a random amount of time

    def make_client(self):
        """ Creates a Kafka client once and sets it """
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except Exception as e:
            logger.warning(f"Failed to create Kafka client: {e}")
            self.client = None
            return False

    def make_consumer(self):
        """ Creates a Kafka consumer once and sets it """
        if self.consumer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic = self.client.topics[self.topic]
            self.consumer = topic.get_simple_consumer(
                consumer_group=b'event_group',
                reset_offset_on_start=False,
                auto_offset_reset=OffsetType.LATEST
            )
            logger.info("Kafka consumer created!")
            return True
        except Exception as e:
            logger.warning(f"Failed to create Kafka consumer: {e}")
            self.client = None
            self.consumer = None
            return False

    def messages(self):
        """ Generator method that catches exceptions in the consumer loop """
        if self.consumer is None:
            self.connect()
        while True:
            try:
                for msg in self.consumer:
                    yield msg
            except Exception as e:
                logger.warning(f"Error while consuming Kafka message: {e}")
                self.client = None
                self.consumer = None
                self.connect()  # Reconnect to Kafka if the consumer fails