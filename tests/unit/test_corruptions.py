"""
Unit tests for src.data_generator.corruptions
"""

import uuid

from src.config.settings import Settings
from src.data_generator import corruptions
from src.data_generator.schemas import Customer, Event, Order, OrderItem, Product
from src.data_generator.utils import SyntheticBatch

# Example objects for each category
EXAMPLE_PRODUCT = Product(
    name="TestProduct",
    price=99.99,
    category="Electronics",
    stock_quantity=10,
    description="A great product.",
)

EXAMPLE_CUSTOMER = Customer(name="John Doe", email="john.doe@example.com")

EXAMPLE_ORDER_ITEM = OrderItem(product_id=uuid.uuid4(), quantity=2, unit_price=49.99)

EXAMPLE_ORDER = Order(
    customer_id=uuid.uuid4(), items=[EXAMPLE_ORDER_ITEM], status="pending", total=99.98
)

EXAMPLE_EVENT = Event(customer_id=uuid.uuid4(), event_type="purchase")


def test_corrupt_product_all_types():
    for corruption_type in corruptions.PRODUCT_CORRUPTIONS:
        product = Product(**EXAMPLE_PRODUCT.__dict__)
        corrupted = corruptions.corrupt_product(product, corruption_type)
        if corruption_type == "missing_name":
            assert corrupted.name is None
        elif corruption_type == "invalid_price":
            assert corrupted.price < 0
        elif corruption_type == "missing_category":
            assert corrupted.category is None
        elif corruption_type == "negative_stock":
            assert corrupted.stock_quantity < 0
        elif corruption_type == "empty_description":
            assert corrupted.description == ""


def test_corrupt_customer_all_types():
    for corruption_type in corruptions.CUSTOMER_CORRUPTIONS:
        customer = Customer(**EXAMPLE_CUSTOMER.__dict__)
        corrupted = corruptions.corrupt_customer(customer, corruption_type)
        if corruption_type == "missing_email":
            assert corrupted.email is None
        elif corruption_type == "invalid_email":
            assert corrupted.email == "not-an-email"
        elif corruption_type == "missing_name":
            assert corrupted.name is None


def test_corrupt_order_all_types():
    for corruption_type in corruptions.ORDER_CORRUPTIONS:
        order = Order(**EXAMPLE_ORDER.__dict__)
        corrupted = corruptions.corrupt_order(order, corruption_type)
        if corruption_type == "missing_customer_id":
            assert corrupted.customer_id is None
        elif corruption_type == "empty_items":
            assert corrupted.items == []
        elif corruption_type == "invalid_status":
            assert not hasattr(corrupted, "status")
        elif corruption_type == "negative_total":
            assert corrupted.total is not None and corrupted.total < 0


def test_corrupt_event_all_types():
    for corruption_type in corruptions.EVENT_CORRUPTIONS:
        event = Event(**EXAMPLE_EVENT.__dict__)
        corrupted = corruptions.corrupt_event(event, corruption_type)
        if corruption_type == "missing_customer_id":
            assert corrupted.customer_id is None
        elif corruption_type == "invalid_event_type":
            assert not hasattr(corrupted, "event_type")


def test_corrupt_batch_all():
    batch = SyntheticBatch(
        products=[Product(**EXAMPLE_PRODUCT.__dict__)],
        customers=[Customer(**EXAMPLE_CUSTOMER.__dict__)],
        orders=[Order(**EXAMPLE_ORDER.__dict__)],
        events=[Event(**EXAMPLE_EVENT.__dict__)],
    )
    # Use real Settings with all probabilities set to 1.0
    settings = Settings()
    settings.CORRUPTION_PROBABILITY_PRODUCT = 1.0
    settings.CORRUPTION_PROBABILITY_CUSTOMER = 1.0
    settings.CORRUPTION_PROBABILITY_ORDER = 1.0
    settings.CORRUPTION_PROBABILITY_EVENT = 1.0
    corrupted = corruptions.corrupt_batch(batch, settings)
    # All objects should be corrupted due to probability=1.0
    assert (
        corrupted.products[0].name is None
        or corrupted.products[0].price < 0
        or corrupted.products[0].category is None
        or corrupted.products[0].stock_quantity < 0
        or corrupted.products[0].description == ""
    )
    assert (
        corrupted.customers[0].email is None
        or corrupted.customers[0].email == "not-an-email"
        or corrupted.customers[0].name is None
    )
    assert (
        corrupted.orders[0].customer_id is None
        or corrupted.orders[0].items == []
        or not hasattr(corrupted.orders[0], "status")
        or (corrupted.orders[0].total is not None and corrupted.orders[0].total < 0)
    )
    assert corrupted.events[0].customer_id is None or not hasattr(
        corrupted.events[0], "event_type"
    )
