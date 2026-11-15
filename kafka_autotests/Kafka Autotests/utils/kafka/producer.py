import json
from uuid import UUID

from kafka import KafkaProducer

from utils.kafka.config import BOOTSTRAP_SERVERS, ENCODING


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


class Producer:
    def __init__(self, topic: str):
        self.topic = topic
        self.value_serializer = lambda v: json.dumps(v, cls=UUIDEncoder).encode(ENCODING)
        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=self.value_serializer
        )

    def send(self, value):
        self.producer.send(self.topic, value)
        self.producer.flush()

    def __del__(self):
        self.producer.close()
