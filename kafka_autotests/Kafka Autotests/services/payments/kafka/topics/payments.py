from services.payments.kafka.models.payment import Payment
from utils.kafka.consumer import Consumer
from utils.kafka.producer import Producer
from utils.kafka.config import PAYMENTS_TOPIC


class PaymentsTopicUtils:
    def __init__(self):
        self.producer = Producer(topic=PAYMENTS_TOPIC)
        self.consumer = Consumer(topic=PAYMENTS_TOPIC)

    def send_payment(self, payment: Payment):
        self.producer.send(value=payment.model_dump())

    def get_payments(self) -> list[Payment]:
        return [Payment(**v) for v in self.consumer.read()]
