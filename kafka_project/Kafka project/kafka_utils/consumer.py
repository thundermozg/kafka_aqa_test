import json

from kafka import KafkaConsumer

from kafka_utils.config import BOOTSTRAP_SERVERS, POLL_TIMEOUT_MS, ENCODING, PAYMENTS_TOPIC


class Consumer:

    def __init__(self):
        self.value_deserializer = lambda v: json.loads(v.decode(ENCODING))
        self.consumer = None

    def start(self):
        self.consumer = KafkaConsumer(
            PAYMENTS_TOPIC,
            enable_auto_commit=True,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=self.value_deserializer,
            auto_offset_reset="earliest",
        )
        return self.consumer

    def read(self):
        records = self.consumer.poll(timeout_ms=POLL_TIMEOUT_MS)
        result = []
        for topic_partition, messages in records.items():
            if topic_partition.topic == PAYMENTS_TOPIC:
                result.extend(messages)
        return result

    def stop(self):
        if self.consumer:
            self.consumer.close()
            self.consumer = None
