from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


class Currency(StrEnum):
    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class Payment(BaseModel):
    id: UUID
    amount: float
    currency: Currency
    description: str = ""
