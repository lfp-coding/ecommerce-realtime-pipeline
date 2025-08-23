"""
Module for injecting realistic data corruption into synthetic e-commerce domain objects.

This module provides functions to randomly corrupt Product, Customer, Order, and Event instances
by introducing typical error types (e.g., missing required fields, invalid values, or inconsistent states).
Batch corruption is supported, using configurable probabilities from the application settings.

Intended for testing data validation, error handling, and robustness of downstream systems.
"""

import random
from typing import TYPE_CHECKING, Optional

from src.config.settings import Settings

from .schemas import Customer, Event, Order, Product

if TYPE_CHECKING:
    from .utils import SyntheticBatch


__all__ = [
    "corrupt_product",
    "corrupt_customer",
    "corrupt_order",
    "corrupt_event",
    "corrupt_batch",
]

# --- Corruption types per object ---
PRODUCT_CORRUPTIONS = [
    "missing_name",
    "invalid_price",
    "missing_category",
    "negative_stock",
    "empty_description",
]
CUSTOMER_CORRUPTIONS = ["missing_email", "invalid_email", "missing_name"]
ORDER_CORRUPTIONS = [
    "missing_customer_id",
    "empty_items",
    "invalid_status",
    "negative_total",
]
EVENT_CORRUPTIONS = ["missing_customer_id", "invalid_event_type"]


# --- Corruption functions ---
def corrupt_product(product: Product, corruption: Optional[str] = None) -> Product:
    if corruption is None:
        corruption = random.choice(PRODUCT_CORRUPTIONS)
    if corruption == "missing_name":
        product.name = None  # type: ignore  # Remove required field
    elif corruption == "invalid_price":
        product.price = -abs(product.price)  # Set invalid negative price
    elif corruption == "missing_category":
        product.category = None  # type: ignore  # Remove required field
    elif corruption == "negative_stock":
        product.stock_quantity = -abs(product.stock_quantity)  # Set negative stock
    elif corruption == "empty_description":
        product.description = ""  # Set empty description
    return product


def corrupt_customer(customer: Customer, corruption: Optional[str] = None) -> Customer:
    if corruption is None:
        corruption = random.choice(CUSTOMER_CORRUPTIONS)
    if corruption == "missing_email":
        customer.email = None  # type: ignore  # Remove required field
    elif corruption == "invalid_email":
        customer.email = "not-an-email"  # Set invalid email
    elif corruption == "missing_name":
        customer.name = None  # type: ignore  # Remove required field
    return customer


def corrupt_order(order: Order, corruption: Optional[str] = None) -> Order:
    if corruption is None:
        corruption = random.choice(ORDER_CORRUPTIONS)
    if corruption == "missing_customer_id":
        order.customer_id = None  # type: ignore  # Remove required field
    elif corruption == "empty_items":
        order.items = []  # Remove all items
    elif corruption == "invalid_status":
        del order.status  # Remove status field entirely
    elif corruption == "negative_total":
        if order.total is not None:
            order.total = -abs(order.total)  # Set negative total
        else:
            order.total = -1.0  # Set negative total fallback
    return order


def corrupt_event(event: Event, corruption: Optional[str] = None) -> Event:
    if corruption is None:
        corruption = random.choice(EVENT_CORRUPTIONS)
    if corruption == "missing_customer_id":
        event.customer_id = None  # type: ignore  # Remove required field
    elif corruption == "invalid_event_type":
        del event.event_type  # Remove event_type field entirely
    return event


# --- Batch corruption ---
def corrupt_batch(batch: "SyntheticBatch", settings: Settings) -> "SyntheticBatch":
    # Products
    for product in batch.products:
        if random.random() < settings.CORRUPTION_PROBABILITY_PRODUCT:
            corrupt_product(product)
    # Customers
    for customer in batch.customers:
        if random.random() < settings.CORRUPTION_PROBABILITY_CUSTOMER:
            corrupt_customer(customer)
    # Orders
    for order in batch.orders:
        if random.random() < settings.CORRUPTION_PROBABILITY_ORDER:
            corrupt_order(order)
    # Events
    for event in batch.events:
        if random.random() < settings.CORRUPTION_PROBABILITY_EVENT:
            corrupt_event(event)
    return batch
