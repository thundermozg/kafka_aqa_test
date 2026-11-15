from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


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


class Receipt(BaseModel):
    receipt_id: UUID
    amount: float = Field(ge=0)
    currency: Currency
    description: str
    status: ReceiptStatus
