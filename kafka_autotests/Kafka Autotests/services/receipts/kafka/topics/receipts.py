from services.receipts.kafka.models.receipt import Receipt
from utils.kafka.config import RECEIPTS_TOPIC
from utils.kafka.consumer import Consumer
from utils.kafka.producer import Producer


class ReceiptsTopicUtils:
    def __init__(self):
        self.producer = Producer(topic=RECEIPTS_TOPIC)
        self.consumer = Consumer(topic=RECEIPTS_TOPIC)

    def send_receipt(self, receipt: Receipt):
        self.producer.send(value=receipt.model_dump())

    def get_receipts(self) -> list[Receipt]:
        return [Receipt(**v) for v in self.consumer.read()]
