from collections.abc import Iterator
from uuid import uuid4

import pytest

from services.payments.kafka.models.payment import Currency, Payment
from services.receipts.kafka.models.receipt import Receipt
from tests.support.payment_receipt_service import PaymentReceiptService


@pytest.fixture
def payment_receipt_service() -> Iterator[PaymentReceiptService]:
    service = PaymentReceiptService()
    try:
        yield service
    finally:
        service.close()


@pytest.fixture
def receipt_scenario(
    payment_receipt_service: PaymentReceiptService,
) -> tuple[Payment, Receipt]:
    payment = Payment(
        id=uuid4(),
        amount=1299.99,
        currency=Currency.USD,
        description="Kafka integration test",
    )
    receipt = payment_receipt_service.create_receipt_for(payment)
    return payment, receipt