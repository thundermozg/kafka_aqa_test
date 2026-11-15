import time
from uuid import uuid4

from services.payments.kafka.models.payment import Payment, Currency
from services.payments.kafka.topics.payments import PaymentsTopicUtils
from services.receipts.kafka.topics.receipts import ReceiptsTopicUtils

p = PaymentsTopicUtils()
r = ReceiptsTopicUtils()

payment = Payment(
    id=uuid4(),
    amount=150,
    currency=Currency.USD,
    description="Aboba"
)
p.send_payment(payment)
time.sleep(10)
receipts = r.get_receipts()
assert len(receipts) == 1
