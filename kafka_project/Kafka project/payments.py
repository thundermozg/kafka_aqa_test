from enum import StrEnum
from typing import List, Dict
from uuid import uuid4, UUID

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict

from kafka_utils.config import PAYMENTS_TOPIC
from kafka_utils.producer import Producer

kafka_producer = Producer()
kafka_producer.start()

app = FastAPI()


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentCreate(BaseModel):
    amount: float
    currency: Currency
    description: str = ""


class Payment(PaymentCreate):
    model_config = ConfigDict(json_encoders={UUID: lambda v: str(v)})

    id: UUID


payments_db: Dict[UUID, Payment] = {}


@app.post("/payments", response_model=Payment)
def create_payment(payment: PaymentCreate):
    payment_id = uuid4()
    new_payment = Payment(
        id=payment_id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
    )
    payments_db[payment_id] = new_payment
    kafka_producer.send(
        topic=PAYMENTS_TOPIC,
        value=new_payment.model_dump()
    )
    return new_payment


@app.get("/payments/{payment_id}", response_model=Payment)
def get_payment(payment_id: UUID):
    payment = payments_db.get(payment_id)
    if not payment:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment


@app.get("/payments", response_model=List[Payment])
def list_payments():
    return list(payments_db.values())


if __name__ == '__main__':
    uvicorn.run("payments:app", host="0.0.0.0", port=8000, reload=True)
