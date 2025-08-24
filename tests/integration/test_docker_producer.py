import json
import time

import pytest
import structlog
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

from src.config.settings import Settings

logger = structlog.get_logger(__name__)


@pytest.fixture(scope="module")
def settings() -> Settings:
    # Use real env/defaults; CI/docker-compose should provide working endpoints.
    return Settings()


@pytest.fixture(scope="module")
def admin_client(settings: Settings) -> AdminClient:
    # Kafka Admin for topic lifecycle used by the test.
    return AdminClient(
        {
            "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "it-docker-producer-admin",
        }
    )


@pytest.fixture(scope="module")
def create_test_topics(admin_client: AdminClient):
    # Create isolated topics for this test file, delete them afterwards.
    import uuid

    suffix = str(uuid.uuid4())[:8]
    topics = [
        (f"it_products_{suffix}", 3, 1),
        (f"it_customers_{suffix}", 3, 1),
        (f"it_orders_{suffix}", 3, 1),
        (f"it_events_{suffix}", 3, 1),
    ]
    topic_names = [name for name, _, _ in topics]
    new_topics = [
        NewTopic(topic=name, num_partitions=parts, replication_factor=repl)
        for name, parts, repl in topics
    ]

    res = admin_client.create_topics(new_topics)
    for name, fut in res.items():
        try:
            fut.result(timeout=20)
            logger.info("topic.created", topic=name)
        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Failed to create topic {name}: {e}")

    time.sleep(2)
    yield topic_names

    cleanup = admin_client.delete_topics(topic_names, operation_timeout=20)
    for name, fut in cleanup.items():
        try:
            fut.result(timeout=20)
            logger.info("topic.deleted", topic=name)
        except Exception:
            # Do not fail teardown if delete races with cluster state
            logger.warning("topic.delete_failed", topic=name)


@pytest.fixture
def consumer(settings: Settings):
    # Generic consumer for reading back produced records.
    conf = {
        "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": f"it-docker-producer-consumer-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    yield c
    c.close()


def _poll_json_messages(
    consumer: Consumer, topics: list[str], expected_total: int, timeout_sec: int = 40
):
    # Polls all topics until expected_total messages are received or timeout occurs.
    consumer.subscribe(topics)
    start = time.time()
    received: dict[str, list[dict]] = {t: [] for t in topics}

    while (
        sum(len(v) for v in received.values()) < expected_total
        and (time.time() - start) < timeout_sec
    ):
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            from confluent_kafka import KafkaError

            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            pytest.fail(f"Consumer error: {msg.error()}")

        try:
            topic = msg.topic()
            payload = json.loads(msg.value().decode("utf-8"))
            received[topic].append(payload)
        except Exception as e:  # noqa: BLE001
            pytest.fail(f"Failed to decode JSON: {e}")

        if sum(len(v) for v in received.values()) % 50 == 0:
            consumer.commit(asynchronous=True)

    consumer.commit(asynchronous=True)
    return received


def test_producer_container_generates_records(
    settings: Settings, create_test_topics, consumer: Consumer
):
    # This test assumes the "producer" container is up (docker compose up -d) and can be invoked via docker exec.
    # It runs the CLI inside the container to produce a deterministic small batch, then verifies consumption.

    product_topic, customer_topic, order_topic, event_topic = create_test_topics

    # Prepare a small batch to keep test fast and deterministic.
    products = 2
    customers = 1
    orders = 2
    events = 3
    seed = 123

    # Command executed inside producer container to publish the batch.
    # The container image must have the project code installed and python entrypoint available.

    topic_env = {
        "KAFKA_PRODUCT_TOPIC": product_topic,
        "KAFKA_CUSTOMER_TOPIC": customer_topic,
        "KAFKA_ORDER_TOPIC": order_topic,
        "KAFKA_EVENT_TOPIC": event_topic,
    }

    # Run docker exec with inline environment variables.
    # Note: "docker exec" does not support --env by default on some Docker versions; use sh -lc to export then run.
    export_env = " ".join(f'export {k}="{v}";' for k, v in topic_env.items())
    full_cmd = [
        "docker",
        "exec",
        "ecommerce-producer",
        "sh",
        "-lc",
        f"{export_env} python -m src.data_generator.producer --products {products} --customers {customers} --orders {orders} --events {events} --seed {seed}",
    ]

    from subprocess import PIPE, run

    result = run(full_cmd, stdout=PIPE, stderr=PIPE, text=True)
    if result.returncode != 0:
        # Surface useful diagnostics to aid debugging in CI
        pytest.fail(
            f"Producer container failed (exit {result.returncode}).\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    # Optional: Parse CLI JSON summary printed by producer for quick sanity checks.
    summary = {}
    try:
        summary = json.loads(result.stdout.strip().splitlines()[-1])
    except Exception:
        # Not fatal if logging noise is present; consumption check below is the source of truth.
        logger.warning(
            "producer.summary.parse_failed", stdout_tail=result.stdout[-500:]
        )

    expected_total = products + customers + orders + events
    if summary:
        assert summary.get("produced_messages") == expected_total

    # Consume from test topics and verify counts.
    topics = [product_topic, customer_topic, order_topic, event_topic]
    received = _poll_json_messages(consumer, topics, expected_total, timeout_sec=60)

    assert len(received[product_topic]) == products
    assert len(received[customer_topic]) == customers
    assert len(received[order_topic]) == orders
    assert len(received[event_topic]) == events

    # Spot-check required fields for each entity type to ensure serialization integrity.
    for p in received[product_topic]:
        assert "product_id" in p and "name" in p and "category" in p and "price" in p

    for c in received[customer_topic]:
        assert "customer_id" in c and "email" in c and "name" in c

    for o in received[order_topic]:
        assert (
            "order_id" in o
            and "customer_id" in o
            and "items" in o
            and isinstance(o["items"], list)
        )

    for e in received[event_topic]:
        assert "event_id" in e and "event_type" in e and "customer_id" in e
