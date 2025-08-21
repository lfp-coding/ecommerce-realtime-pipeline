"""
Integration test for DataProducer:
- Creates required topics (products, customers, orders, events) with replication/partitions.
- Runs the real DataProducer to publish a small synthetic batch.
- Consumes from all topics and validates delivery, count, and JSON structure.
"""

import json
import time

import pytest
import structlog
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from src.config.logging_config import configure_logging
from src.config.settings import Settings
from src.data_generator.producer import DataProducer

logger = structlog.get_logger(__name__)
configure_logging(Settings())


@pytest.fixture(scope="module")
def settings():
    return Settings()


@pytest.fixture(scope="module")
def admin_client(settings):
    return AdminClient(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "it-producer-test-admin",
        }
    )


@pytest.fixture(scope="module")
def ensure_topics(admin_client, settings):
    """
    Ensures that the four domain topics exist.
    Creates them with sensible defaults if not present.
    """
    topics = [
        (settings.KAFKA_PRODUCT_TOPIC, 3, 3),
        (settings.KAFKA_CUSTOMER_TOPIC, 3, 3),
        (settings.KAFKA_ORDER_TOPIC, 3, 3),
        (settings.KAFKA_EVENT_TOPIC, 3, 3),
    ]

    metadata = admin_client.list_topics(timeout=10)
    to_create = []
    for name, partitions, repl in topics:
        if name not in metadata.topics:
            to_create.append(
                NewTopic(topic=name, num_partitions=partitions, replication_factor=repl)
            )

    if to_create:
        res = admin_client.create_topics(to_create)
        for name, fut in res.items():
            try:
                fut.result(timeout=15)
                logger.info("topic.created", topic=name)
            except Exception as e:
                # If created in parallel by another test: ignorable error, but log it
                logger.warning("topic.create_failed", topic=name, error=str(e))
        # Short wait until topics are fully available
        time.sleep(2)

    yield

    # No cleanup on purpose, as these are "system topics" for the project.


@pytest.fixture
def consumer(settings):
    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"it-producer-consumer-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    yield c
    c.close()


def _poll_messages(consumer, topics, expected_total, timeout_sec=20):
    """
    Consumes until expected_total messages arrive or timeout is reached.
    Returns dict topic->list(parsed_messages).
    """
    consumer.subscribe(topics)
    start = time.time()
    received = {t: [] for t in topics}

    while (
        sum(len(v) for v in received.values()) < expected_total
        and (time.time() - start) < timeout_sec
    ):
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            # Ignore EOFs; fail hard on other errors
            from confluent_kafka import KafkaError

            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                pytest.fail(f"Consumer error: {msg.error()}")

        topic = msg.topic()
        try:
            val = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            pytest.fail(f"Failed to parse JSON from topic {topic}: {e}")

        received[topic].append(val)

        # Commit in batches
        if sum(len(v) for v in received.values()) % 50 == 0:
            consumer.commit(asynchronous=True)

    # Final commit
    consumer.commit(asynchronous=True)
    return received


class TestDataProducerIntegration:
    def test_data_producer_end_to_end(self, settings, ensure_topics, consumer):
        """
        End-to-end Test:
        - produce_batch() sends a small number of entities
        - Consumer verifies arrival and basic field structure
        """
        # Keep small for fast tests
        product_count = 3
        customer_count = 2
        order_count = 5
        event_count = 7
        seed = 123

        producer = DataProducer(settings)
        # Before the test: reset metrics to 0
        assert producer.metrics.produced_messages == 0

        # Produce Batch
        metrics = producer.produce_batch(
            product_count=product_count,
            customer_count=customer_count,
            order_count=order_count,
            event_count=event_count,
            seed=seed,
            flush=True,
        )

        expected_total = product_count + customer_count + order_count + event_count
        assert metrics.produced_messages == expected_total, "Producer metrics mismatch"

        topics = [
            settings.KAFKA_PRODUCT_TOPIC,
            settings.KAFKA_CUSTOMER_TOPIC,
            settings.KAFKA_ORDER_TOPIC,
            settings.KAFKA_EVENT_TOPIC,
        ]

        # Consume and validate
        received = _poll_messages(consumer, topics, expected_total, timeout_sec=30)

        # Basic checks: count per topic > 0 and sum == expected_total
        assert len(received[settings.KAFKA_PRODUCT_TOPIC]) == product_count
        assert len(received[settings.KAFKA_CUSTOMER_TOPIC]) == customer_count
        assert len(received[settings.KAFKA_ORDER_TOPIC]) == order_count
        assert len(received[settings.KAFKA_EVENT_TOPIC]) == event_count
        total_received = sum(len(v) for v in received.values())
        assert total_received == expected_total

        # Field checks (lightweight, schema-near)

        # Products
        for p in received[settings.KAFKA_PRODUCT_TOPIC]:
            assert (
                "product_id" in p and "name" in p and "category" in p and "price" in p
            )
            assert isinstance(p["price"], (int, float))

        # Customers
        for c in received[settings.KAFKA_CUSTOMER_TOPIC]:
            assert "customer_id" in c and "email" in c and "name" in c

        # Orders
        for o in received[settings.KAFKA_ORDER_TOPIC]:
            assert (
                "order_id" in o and "customer_id" in o and "items" in o and "total" in o
            )
            assert isinstance(o["items"], list)
            assert isinstance(o["total"], (int, float))
            # At least one item and each item has product_id, quantity, unit_price
            assert len(o["items"]) >= 1
            for it in o["items"]:
                assert "product_id" in it and "quantity" in it and "unit_price" in it

        # Events
        for e in received[settings.KAFKA_EVENT_TOPIC]:
            assert "event_id" in e and "event_type" in e and "customer_id" in e

        logger.info(
            "producer.integration.ok",
            produced=metrics.produced_messages,
            received=total_received,
            topics=topics,
        )
