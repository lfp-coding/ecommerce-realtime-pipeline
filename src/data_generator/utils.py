"""Utility functions for generating synthetic e-commerce domain objects.

The functions in this module return Pydantic model instances defined in
`schemas.py`. They intentionally avoid external dependencies (e.g. Faker)
to keep the data generator lightweight. Minimal randomised data is
produced using Python's standard library.

Typical usage:

        from data_generator import utils as dg

        product = dg.generate_product()
        customer = dg.generate_customer()
        order = dg.generate_order([product], customer_id=customer.customer_id)
        event = dg.generate_event(customer_id=customer.customer_id)

Batch helpers (`generate_products`, `generate_customers`, etc.) are also
provided for convenience.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from typing import List, Literal, Sequence, cast

from .schemas import (
    Customer,
    Event,
    Order,
    OrderItem,
    Product,
)

__all__ = [
    "set_random_seed",
    "generate_product",
    "generate_products",
    "generate_customer",
    "generate_customers",
    "generate_order_item",
    "generate_order_items",
    "generate_order",
    "generate_orders",
    "generate_event",
    "generate_events",
]


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------
def set_random_seed(seed: int | None) -> None:
    """Set the global random seed (deterministic generation)."""

    if seed is not None:
        random.seed(seed)


# ---------------------------------------------------------------------------
# Internal helpers (not exported)
# ---------------------------------------------------------------------------
_PRODUCT_CATEGORIES: Sequence[str] = (
    "electronics",
    "fashion",
    "books",
    "home",
    "sports",
    "toys",
    "beauty",
    "grocery",
)

_EVENT_TYPES: Sequence[str] = (
    "page_view",
    "product_view",
    "add_to_cart",
    "remove_from_cart",
    "purchase",
)


def _random_name(prefix: str, words: int = 2) -> str:
    syllables = [
        "al",
        "ver",
        "tek",
        "zon",
        "lum",
        "nex",
        "tri",
        "quo",
        "plex",
        "gen",
    ]
    parts = [
        "".join(random.choice(syllables) for _ in range(1, 2)) for _ in range(words)
    ]
    base = " ".join(part.capitalize() for part in parts)
    return f"{prefix} {base}".strip()


def _random_sentence(min_words: int = 5, max_words: int = 12) -> str:
    vocab = [
        "innovative",
        "lightweight",
        "durable",
        "premium",
        "eco",
        "wireless",
        "smart",
        "ergonomic",
        "portable",
        "sleek",
        "versatile",
        "compact",
        "high-performance",
    ]
    n = random.randint(min_words, max_words)
    return " ".join(random.choice(vocab) for _ in range(n)).capitalize() + "."


def _random_email(name: str) -> str:
    user = name.lower().replace(" ", ".")
    domain = random.choice(["example.com", "shop.test", "mail.local"])
    return f"{user}@{domain}"


def _random_price(min_price: float = 2.0, max_price: float = 500.0) -> float:
    return round(random.uniform(min_price, max_price), 2)


def _random_stock(max_qty: int = 500) -> int:
    return random.randint(0, max_qty)


# ---------------------------------------------------------------------------
# Generation functions
# ---------------------------------------------------------------------------
def generate_product(
    *,
    name: str | None = None,
    category: str | None = None,
    price: float | None = None,
    description: str | None = None,
    stock_quantity: int | None = None,
) -> Product:
    """Generate a single random `Product`.

    Field overrides can be supplied; unknown fields are ignored.
    """

    name = name or _random_name("Product")
    category = category or random.choice(_PRODUCT_CATEGORIES)
    price = price if price is not None else _random_price()
    description = description or _random_sentence()
    stock_quantity = stock_quantity if stock_quantity is not None else _random_stock()

    return Product(
        name=name,
        category=category,
        price=price,
        description=description,
        stock_quantity=stock_quantity or 0,
    )


def generate_products(n: int, **overrides) -> List[Product]:
    """Generate a list of `n` products."""

    return [generate_product(**overrides) for _ in range(n)]


def generate_customer(*, name: str | None = None, email: str | None = None) -> Customer:
    """Generate a single random `Customer`. Email is derived from name if omitted."""

    name = name or _random_name("Customer")
    email = email or _random_email(name)
    return Customer(name=name, email=email)


def generate_customers(n: int, **overrides) -> List[Customer]:
    """Generate a list of `n` customers with optional field overrides."""
    return [generate_customer(**overrides) for _ in range(n)]


def generate_order_item(
    *, product: Product | None = None, quantity: int | None = None
) -> OrderItem:
    """Generate a random `OrderItem`.

    If a product is provided, its product_id and price are used.
    Otherwise a synthetic price & UUID are generated.
    """

    product_id = product.product_id if product else uuid.uuid4()
    unit_price = product.price if product else _random_price()
    quantity = quantity or random.randint(1, 5)
    return OrderItem(product_id=product_id, unit_price=unit_price, quantity=quantity)


def generate_order_items(
    n: int,
    *,
    products: Sequence[Product] | None = None,
) -> List[OrderItem]:
    """Generate a list of `n` order items."""
    items: List[OrderItem] = []
    for _ in range(n):
        product = random.choice(products) if products else None
        items.append(generate_order_item(product=product))
    return items


def generate_order(
    products: Sequence[Product] | None = None,
    *,
    customer_id: uuid.UUID | None = None,
    min_items: int = 1,
    max_items: int = 5,
    status: str | None = None,
) -> Order:
    """Generate a random `Order`.

    If `products` is provided, items will reference those product IDs.
    `customer_id` defaults to a new UUID if not provided.
    """

    if min_items <= 0:
        raise ValueError("min_items must be >= 1")
    if max_items < min_items:
        raise ValueError("max_items must be >= min_items")

    item_count = random.randint(min_items, max_items)
    items = generate_order_items(item_count, products=products)
    resolved_status: Literal[
        "pending", "processing", "shipped", "delivered", "cancelled"
    ] = cast(
        Literal["pending", "processing", "shipped", "delivered", "cancelled"],
        status or "pending",
    )
    order = Order(
        customer_id=customer_id or uuid.uuid4(), items=items, status=resolved_status
    )
    order.compute_total()
    return order


def generate_orders(
    n: int,
    *,
    products: Sequence[Product] | None = None,
    customer_ids: Sequence[uuid.UUID] | None = None,
    **kwargs,
) -> List[Order]:
    """Generate a list of `n` orders with optional field overrides."""
    if customer_ids and not isinstance(customer_ids, Sequence):  # defensive
        customer_ids = list(customer_ids)
    return [
        generate_order(
            products,
            customer_id=(random.choice(customer_ids) if customer_ids else None),
            **kwargs,
        )
        for _ in range(n)
    ]


def generate_event(
    *,
    customer_id: uuid.UUID | None = None,
    event_type: str | None = None,
) -> Event:
    """Generate a random `Event` (user behaviour)."""

    event_type = event_type or random.choice(_EVENT_TYPES)
    return Event(customer_id=customer_id or uuid.uuid4(), event_type=event_type)  # type: ignore[arg-type]


def generate_events(
    n: int,
    *,
    customer_ids: Sequence[uuid.UUID] | None = None,
    event_type: str | None = None,
) -> List[Event]:
    """Generate a list of `n` events with optional field overrides."""
    return [
        generate_event(
            customer_id=(random.choice(customer_ids) if customer_ids else None),
            event_type=event_type,
        )
        for _ in range(n)
    ]


# ---------------------------------------------------------------------------
# Simple dataclass batches (optional convenience wrappers)
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class SyntheticBatch:
    """Container for a coherent batch of generated objects."""

    products: List[Product]
    customers: List[Customer]
    orders: List[Order]
    events: List[Event]


def generate_batch(
    *,
    product_count: int = 10,
    customer_count: int = 10,
    order_count: int = 25,
    event_count: int = 50,
    seed: int | None = None,
) -> SyntheticBatch:
    """Generate a coherent batch of products, customers, orders & events."""

    set_random_seed(seed)
    products = generate_products(product_count)
    customers = generate_customers(customer_count)
    customer_ids = [c.customer_id for c in customers]
    orders = generate_orders(order_count, products=products, customer_ids=customer_ids)
    events = generate_events(event_count, customer_ids=customer_ids)
    return SyntheticBatch(products, customers, orders, events)
