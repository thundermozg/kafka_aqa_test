import json
from uuid import UUID

from kafka import KafkaProducer

from kafka_utils.config import BOOTSTRAP_SERVERS, ENCODING


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class Producer:
    def __init__(self):
        self.value_serializer = lambda v: json.dumps(v, cls=UUIDEncoder).encode(ENCODING)
        self.producer = None

    def start(self):
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=self.value_serializer
        )
        return self.producer

    def send(self, topic, value):
        if self.producer:
            self.producer.send(topic, value)
            self.producer.flush()

    def stop(self):
        if self.producer:
            self.producer.close()
            self.producer = None
