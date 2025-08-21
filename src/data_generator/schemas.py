"""Pydantic models for synthetic e-commerce data generation (products, customers,
orders, events). All timestamps are UTC. Unknown fields are rejected (extra=forbid)."""

import json
import uuid
from datetime import datetime, timezone
from typing import List, Literal

from pydantic import BaseModel, Field, PositiveInt


class Product(BaseModel):
    """Product data schema."""

    product_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    name: str
    category: str
    price: float
    description: str | None = None
    stock_quantity: PositiveInt = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"extra": "forbid"}

    def to_dict(self) -> dict:
        return self.model_dump()

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.model_dump(), default=str).encode("utf-8")


class Customer(BaseModel):
    """Customer data schema."""

    customer_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    email: str
    name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"extra": "forbid"}

    def to_dict(self) -> dict:
        return self.model_dump()

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.model_dump(), default=str).encode("utf-8")


class OrderItem(BaseModel):
    """Order item data schema."""

    product_id: uuid.UUID
    quantity: PositiveInt = 1
    unit_price: float

    model_config = {"extra": "forbid"}

    def line_total(self) -> float:
        return round(self.quantity * self.unit_price, 2)


class Order(BaseModel):
    """Order data schema."""

    order_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    customer_id: uuid.UUID
    items: List[OrderItem]
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total: float | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: Literal["pending", "processing", "shipped", "delivered", "cancelled"] = (
        "pending"
    )

    model_config = {"extra": "forbid"}

    def compute_total(self) -> float:
        total = sum(item.line_total() for item in self.items)
        self.total = round(total, 2)
        return self.total

    def to_dict(self) -> dict:
        # ensure total is computed before dumping
        if self.total is None:
            self.compute_total()
        return self.model_dump()

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), default=str).encode("utf-8")


class Event(BaseModel):
    """Event data schema for tracking user behavior."""

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    event_type: Literal[
        "page_view", "product_view", "add_to_cart", "remove_from_cart", "purchase"
    ]
    customer_id: uuid.UUID
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"extra": "forbid"}

    def to_dict(self) -> dict:
        return self.model_dump()

    def to_json_bytes(self) -> bytes:
        return json.dumps(self.to_dict(), default=str).encode("utf-8")
