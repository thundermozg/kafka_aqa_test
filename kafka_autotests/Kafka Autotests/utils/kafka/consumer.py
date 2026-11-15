import json

from kafka import KafkaConsumer

from utils.kafka.config import BOOTSTRAP_SERVERS, ENCODING


class Consumer:
    POLL_TIMEOUT_MS = 1_000

    def __init__(self, topic: str):
        self.value_deserializer = lambda v: json.loads(v.decode(ENCODING))
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=self.value_deserializer,
        )
        self.consumer.poll(self.POLL_TIMEOUT_MS) # assignment

    def read(self):
        records = self.consumer.poll(self.POLL_TIMEOUT_MS)
        all_values = []
        for partition_records in records.values():
            for record in partition_records:
                all_values.append(record.value)
        return all_values

    def __del__(self):
        self.consumer.close()
