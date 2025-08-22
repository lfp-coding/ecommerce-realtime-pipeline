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
def create_test_topics(admin_client):
    """
    Creates temporary test topics for integration tests and deletes them after tests.
    """
    import uuid

    topic_suffix = str(uuid.uuid4())[:8]
    topics = [
        (f"test_products_{topic_suffix}", 3, 1),
        (f"test_customers_{topic_suffix}", 3, 1),
        (f"test_orders_{topic_suffix}", 3, 1),
        (f"test_events_{topic_suffix}", 3, 1),
    ]
    topic_names = [name for name, _, _ in topics]
    to_create = [
        NewTopic(topic=name, num_partitions=partitions, replication_factor=repl)
        for name, partitions, repl in topics
    ]
    res = admin_client.create_topics(to_create)
    for name, fut in res.items():
        try:
            fut.result(timeout=15)
            logger.info("topic.created", topic=name)
        except Exception as e:
            logger.warning("topic.create_failed", topic=name, error=str(e))
    time.sleep(2)
    yield topic_names
    # Cleanup: delete topics
    del_res = admin_client.delete_topics(topic_names, operation_timeout=15)
    for name, fut in del_res.items():
        try:
            fut.result(timeout=15)
            logger.info("topic.deleted", topic=name)
        except Exception as e:
            logger.warning("topic.delete_failed", topic=name, error=str(e))
    time.sleep(2)


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
    def test_data_producer_end_to_end(self, settings, create_test_topics, consumer):
        """
        End-to-end Test:
        - produce_batch() sends a small number of entities to test topics
        - Consumer verifies arrival and basic field structure
        """
        product_count = 3
        customer_count = 2
        order_count = 5
        event_count = 7
        seed = 123

        # Map test topics to settings for the producer
        product_topic, customer_topic, order_topic, event_topic = create_test_topics

        # Patch settings to use test topics
        settings.KAFKA_PRODUCT_TOPIC = product_topic
        settings.KAFKA_CUSTOMER_TOPIC = customer_topic
        settings.KAFKA_ORDER_TOPIC = order_topic
        settings.KAFKA_EVENT_TOPIC = event_topic

        producer = DataProducer(settings)
        assert producer.metrics.produced_messages == 0

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

        topics = [product_topic, customer_topic, order_topic, event_topic]
        received = _poll_messages(consumer, topics, expected_total, timeout_sec=30)

        assert len(received[product_topic]) == product_count
        assert len(received[customer_topic]) == customer_count
        assert len(received[order_topic]) == order_count
        assert len(received[event_topic]) == event_count
        total_received = sum(len(v) for v in received.values())
        assert total_received == expected_total

        for p in received[product_topic]:
            assert (
                "product_id" in p and "name" in p and "category" in p and "price" in p
            )
            assert isinstance(p["price"], (int, float))

        for c in received[customer_topic]:
            assert "customer_id" in c and "email" in c and "name" in c

        for o in received[order_topic]:
            assert (
                "order_id" in o and "customer_id" in o and "items" in o and "total" in o
            )
            assert isinstance(o["items"], list)
            assert isinstance(o["total"], (int, float))
            # At least one item and each item has product_id, quantity, unit_price
            assert len(o["items"]) >= 1
            for it in o["items"]:
                assert "product_id" in it and "quantity" in it and "unit_price" in it

        for e in received[event_topic]:
            assert "event_id" in e and "event_type" in e and "customer_id" in e

        logger.info(
            "producer.integration.ok",
            produced=metrics.produced_messages,
            received=total_received,
            topics=topics,
        )
