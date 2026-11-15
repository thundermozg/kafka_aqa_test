import time

from services.payments.kafka.models.payment import Payment
from services.payments.kafka.topics.payments import PaymentsTopicUtils
from services.receipts.kafka.models.receipt import Receipt
from services.receipts.kafka.topics.receipts import ReceiptsTopicUtils


class PaymentReceiptService:
    RECEIPT_TIMEOUT_SECONDS = 35

    def __init__(self) -> None:
        self._payments = PaymentsTopicUtils()
        # Создаём consumer до отправки платежа.
        self._receipts = ReceiptsTopicUtils()

    def create_receipt_for(self, payment: Payment) -> Receipt:
        self._payments.send_payment(payment)
        deadline = time.monotonic() + self.RECEIPT_TIMEOUT_SECONDS

        while time.monotonic() < deadline:
            for receipt in self._receipts.get_receipts():
                if receipt.receipt_id == payment.id:
                    return receipt

        raise TimeoutError(
            f"Receipt for payment {payment.id} was not received "
            f"within {self.RECEIPT_TIMEOUT_SECONDS} seconds"
        )

    def close(self) -> None:
        self._payments.close()
        self._receipts.close()