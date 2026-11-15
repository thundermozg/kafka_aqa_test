import asyncio
from contextlib import asynccontextmanager
from enum import StrEnum
from uuid import UUID

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel, Field

from kafka_utils.config import RECEIPTS_TOPIC
from kafka_utils.consumer import Consumer
from kafka_utils.producer import Producer


POLL_FREQUENCY = 10


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class ReceiptStatus(StrEnum):
    PAID = "paid"
    PENDING = "pending"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REFUNDED = "refunded"


producer = Producer()
consumer = Consumer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer.start()
    consumer.start()

    task = asyncio.create_task(generate_receipts_periodically())

    yield

    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

app = FastAPI(lifespan=lifespan)

class Payment(BaseModel):
    id: UUID
    amount: float
    currency: Currency
    description: str = ""


class Receipt(BaseModel):
    receipt_id: UUID
    amount: float = Field(ge=0)
    currency: Currency
    description: str
    status: ReceiptStatus


def generate_receipt(payment: Payment) -> Receipt:
    receipt = Receipt(
        receipt_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        status=ReceiptStatus.PAID,
    )
    return receipt


def _generate_receipts() -> list[Receipt]:
    messages = consumer.read()
    if not messages:
        return []

    receipts = []
    for msg in messages:
        payment = Payment(**msg.value)
        receipt = Receipt(
            receipt_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            status=ReceiptStatus.PAID,
        )
        receipts.append(receipt)
    return receipts


def _process_receipts():
    receipts = _generate_receipts()

    for receipt in receipts:
        producer.send(RECEIPTS_TOPIC, receipt.model_dump())

    return {"count": len(receipts)}


@app.post("/generate-receipts")
def generate_receipts():
    return _process_receipts()


async def generate_receipts_periodically():
    while True:
        _process_receipts()
        await asyncio.sleep(POLL_FREQUENCY)


if __name__ == '__main__':
    uvicorn.run("receipts:app", host="0.0.0.0", port=8000, reload=True)
