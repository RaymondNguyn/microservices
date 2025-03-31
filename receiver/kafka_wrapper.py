import time
import random
import json
from pykafka import KafkaClient
from pykafka.exceptions import KafkaException
import logging

logger = logging.getLogger('basicLogger')

class KafkaWrapper:
    def __init__(self, hostname, topic):
        self.hostname = hostname
        self.topic = topic
        self.client = None
        self.producer = None
        self.connect()

    def connect(self):
        """Infinite loop: will keep trying to connect to the Kafka producer."""
        while True:
            logger.debug("Trying to connect to Kafka producer...")
            if self.make_client():
                if self.make_producer():
                    break
            # Sleeps for a random amount of time (0.5 to 1.5s)
            time.sleep(random.randint(500, 1500) / 1000)

    def make_client(self):
        """
        Runs once, makes a client and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.client is not None:
            return True
        try:
            self.client = KafkaClient(hosts=self.hostname)
            logger.info("Kafka client created!")
            return True
        except KafkaException as e:
            msg = f"Kafka error when making client: {e}"
            logger.warning(msg)
            self.client = None
            return False

    def make_producer(self):
        """
        Runs once, makes a producer and sets it on the instance.
        Returns: True (success), False (failure)
        """
        if self.producer is not None:
            return True
        if self.client is None:
            return False
        try:
            topic = self.client.topics[self.topic]
            self.producer = topic.get_sync_producer()
            logger.info("Kafka producer created!")
            return True
        except KafkaException as e:
            msg = f"Error when creating producer: {e}"
            logger.warning(msg)
            self.producer = None
            return False

    def produce_message(self, message):
        """
        Produce a message to the Kafka topic.
        """
        if self.producer is None:
            self.connect()
        
        try:
            msg_str = json.dumps(message)
            self.producer.produce(msg_str.encode('utf-8'))
            logger.info(f"Message produced: {msg_str}")
        except KafkaException as e:
            msg = f"Error when producing message: {e}"
            logger.warning(msg)

