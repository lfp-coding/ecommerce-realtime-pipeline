"""
Integration tests for modern Kafka KRaft infrastructure
Validates cluster health, topic management, and message flow
"""

import json
import time

import pytest
import structlog
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

from src.config.settings import Settings

logger = structlog.get_logger(__name__)


@pytest.fixture(scope="module")
def settings():
    return Settings()


class TestKafkaInfrastructure:
    """Test Kafka KRaft cluster functionality and performance"""

    @pytest.fixture(scope="class")
    def kafka_config(self, settings):
        """Kafka connection configuration"""
        bootstrap_servers = settings.KAFKA_BOOTSTRAP_SERVERS
        return {"bootstrap.servers": bootstrap_servers, "client.id": "test-client"}

    @pytest.fixture(scope="class")
    def admin_client(self, kafka_config):
        """Kafka admin client for cluster management"""
        return AdminClient(kafka_config)

    def test_kafka_cluster_health(self, admin_client):
        """Verify Kafka cluster is healthy and accessible"""
        metadata = admin_client.list_topics(timeout=10)
        assert len(metadata.brokers) >= 1, "No Kafka brokers available"
        assert metadata.cluster_id is not None, "Cluster ID should be set in KRaft mode"
        for broker in metadata.brokers.values():
            assert broker.id >= 0, "Broker should have valid ID"
        logger.info(f"✅ Cluster healthy with {len(metadata.brokers)} brokers")
        logger.info(f"   Cluster ID: {metadata.cluster_id}")

    def test_kraft_controller_availability(self, admin_client):
        """Verify KRaft controller is functioning"""
        test_topic = f"test-kraft-{int(time.time())}"
        topic = NewTopic(topic=test_topic, num_partitions=1, replication_factor=3)
        result = admin_client.create_topics([topic])
        for topic_name, future in result.items():
            try:
                future.result(timeout=10)
                logger.info(
                    f"✅ Successfully created topic {topic_name} via KRaft controller"
                )
            except Exception as e:
                pytest.fail(f"Failed to create topic via controller: {e}")
        delete_result = admin_client.delete_topics([test_topic])
        for topic_name, future in delete_result.items():
            try:
                future.result(timeout=10)
                logger.info(f"✅ Deleted topic: {topic_name}")
            except Exception as e:
                logger.warning(f"⚠️ Warning: Could not delete topic {topic_name}: {e}")

    def test_topic_creation_and_configuration(self, admin_client):
        """Test topic creation with specific configurations"""
        test_topics = [
            {
                "name": f"test-customers-{int(time.time())}",
                "partitions": 3,
                "replication_factor": 3,
                "config": {"cleanup.policy": "compact", "retention.ms": "604800000"},
            },
            {
                "name": f"test-events-{int(time.time())}",
                "partitions": 6,
                "replication_factor": 3,
                "config": {"cleanup.policy": "delete", "retention.ms": "86400000"},
            },
        ]
        created_topics = []
        try:
            for topic_config in test_topics:
                topic = NewTopic(
                    topic=topic_config["name"],
                    num_partitions=topic_config["partitions"],
                    replication_factor=topic_config["replication_factor"],
                    config=topic_config["config"],
                )
                result = admin_client.create_topics([topic])
                for topic_name, future in result.items():
                    future.result(timeout=10)
                    created_topics.append(topic_name)
                    logger.info(f"✅ Created topic: {topic_name}")
            # Wait for topics to be fully available
            time.sleep(2)
            for topic_config in test_topics:
                topic_name = topic_config["name"]
                metadata = admin_client.list_topics(timeout=10)
                topic_metadata = metadata.topics[topic_name]
                assert len(topic_metadata.partitions) == topic_config["partitions"], (
                    f"Topic {topic_name} should have {topic_config['partitions']} partitions"
                )
                resource = ConfigResource(ResourceType.TOPIC, topic_name)
                configs = admin_client.describe_configs([resource])
                topic_configs = configs[resource].result(timeout=10)
                for config_key, config_value in topic_config["config"].items():
                    actual_value = topic_configs[config_key].value
                    assert actual_value == config_value, (
                        f"Config {config_key} should be {config_value}, got {actual_value}"
                    )
                logger.info(f"✅ Verified configuration for topic: {topic_name}")
        finally:
            if created_topics:
                admin_client.delete_topics(created_topics)

    def test_producer_consumer_message_flow(self, admin_client, kafka_config):
        """Test end-to-end message flow with proper serialization"""
        test_topic = f"test-message-flow-{int(time.time())}"
        topic = NewTopic(topic=test_topic, num_partitions=3, replication_factor=3)
        result = admin_client.create_topics([topic])
        result[test_topic].result(timeout=10)
        try:
            test_messages = [
                {
                    "customer_id": "cust-001",
                    "email": "test1@example.com",
                    "amount": 99.99,
                },
                {
                    "customer_id": "cust-002",
                    "email": "test2@example.com",
                    "amount": 149.50,
                },
                {
                    "order_id": "order-001",
                    "items": [{"product": "laptop", "quantity": 1}],
                },
            ]
            producer_config = {
                **kafka_config,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 3,
                "max.in.flight.requests.per.connection": 1,
            }
            producer = Producer(producer_config)
            sent_messages = []
            for i, message in enumerate(test_messages):
                message_key = f"key-{i}"
                message_value = json.dumps(message)
                producer.produce(
                    topic=test_topic,
                    key=message_key,
                    value=message_value,
                    callback=self._delivery_callback,
                )
                sent_messages.append((message_key, message))
            producer.flush(timeout=10)
            consumer_config = {
                **kafka_config,
                "group.id": f"test-group-{int(time.time())}",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
            consumer = Consumer(consumer_config)
            consumer.subscribe([test_topic])
            received_messages = []
            start_time = time.time()
            timeout = 30
            while len(received_messages) < len(test_messages):
                if time.time() - start_time > timeout:
                    break
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        pytest.fail(f"Consumer error: {msg.error()}")
                try:
                    key = msg.key().decode("utf-8") if msg.key() else None
                    value = json.loads(msg.value().decode("utf-8"))
                    received_messages.append((key, value))
                    # Commit offsets in batches for better throughput
                    if len(received_messages) % 10 == 0 or len(
                        received_messages
                    ) == len(test_messages):
                        consumer.commit()
                except Exception as e:
                    pytest.fail(f"Failed to parse message: {e}")
            consumer.close()
            assert len(received_messages) == len(test_messages), (
                f"Should receive {len(test_messages)} messages, got {len(received_messages)}"
            )
            sent_keys = {key for key, _ in sent_messages}
            received_keys = {key for key, _ in received_messages}
            assert sent_keys == received_keys, "All message keys should be received"
            logger.info(
                f"✅ Successfully sent and received {len(test_messages)} messages"
            )
        finally:
            admin_client.delete_topics([test_topic])

    def _delivery_callback(self, err, msg):
        """Callback for producer message delivery"""
        if err:
            logger.error(f"❌ Message delivery failed: {err}")
        else:
            logger.info(
                f"✅ Message delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}"
            )

    def test_kafka_performance_basic(self, kafka_config):
        """Basic performance test for message throughput"""
        test_topic = f"test-performance-{int(time.time())}"
        admin_client = AdminClient(kafka_config)
        topic = NewTopic(topic=test_topic, num_partitions=6, replication_factor=3)
        result = admin_client.create_topics([topic])
        result[test_topic].result(timeout=10)
        try:
            num_messages = 1000
            message_size = 1024
            producer_config = {
                **kafka_config,
                "batch.size": 16384,
                "linger.ms": 5,
                "compression.type": "lz4",
            }
            producer = Producer(producer_config)
            test_message = "x" * message_size
            start_time = time.time()
            for i in range(num_messages):
                producer.produce(
                    topic=test_topic, key=f"perf-key-{i}", value=test_message
                )
            producer.flush(timeout=30)
            end_time = time.time()
            duration = end_time - start_time
            throughput = num_messages / duration
            data_rate = (num_messages * message_size) / duration / 1024 / 1024
            logger.info("✅ Performance test results:")
            logger.info(f"   Messages: {num_messages}")
            logger.info(f"   Duration: {duration:.2f} seconds")
            logger.info(f"   Throughput: {throughput:.2f} messages/sec")
            logger.info(f"   Data rate: {data_rate:.2f} MB/sec")
            assert throughput > 100, f"Throughput too low: {throughput:.2f} msg/sec"
            assert data_rate > 0.1, f"Data rate too low: {data_rate:.2f} MB/sec"
        finally:
            admin_client.delete_topics([test_topic])

    def test_kafka_topic_management_operations(self, admin_client):
        """
        Tests the full lifecycle of Kafka topics: creation, incremental alteration, and deletion.
        """
        base_topic_name = f"test-mgmt-{int(time.time())}"
        topics_to_create = [
            NewTopic(
                topic=f"{base_topic_name}-1",
                num_partitions=1,
                replication_factor=3,
                config={"retention.ms": "3600000"},
            ),
            NewTopic(
                topic=f"{base_topic_name}-2",
                num_partitions=3,
                replication_factor=3,
                config={"cleanup.policy": "compact"},
            ),
        ]
        created_topics = []

        try:
            # 1. Create topics
            create_result = admin_client.create_topics(topics_to_create)
            for topic_name, future in create_result.items():
                future.result(timeout=10)
                created_topics.append(topic_name)
                logger.info(f"✅ Created topic: {topic_name}")

            time.sleep(2)  # Wait for topics to be fully available

            # 2. Verify topic existence
            metadata = admin_client.list_topics(timeout=10)
            for topic_name in created_topics:
                assert topic_name in metadata.topics, f"Topic {topic_name} should exist"

            # 3. Incrementally alter the configuration of the first topic
            resource = ConfigResource(ResourceType.TOPIC, created_topics[0])
            config_entry = ConfigEntry(
                name="retention.ms",
                value="7200000",
                incremental_operation=AlterConfigOpType.SET,
            )
            resource.add_incremental_config(config_entry)

            alter_result = admin_client.incremental_alter_configs([resource])
            alter_result[resource].result(timeout=10)
            logger.info(f"✅ Submitted config update for topic: {created_topics}")

            # Allow time for the configuration change to propagate across the cluster
            time.sleep(5)

            # 4. Verify the configuration change
            configs_after = admin_client.describe_configs([resource])
            updated_config = configs_after[resource].result(timeout=10)
            updated_value = updated_config["retention.ms"].value

            assert updated_value == "7200000", (
                f"Expected retention.ms to be '7200000', but got '{updated_value}'"
            )
            logger.info(f"✅ Verified updated config: retention.ms = {updated_value}")

        finally:
            # 5. Clean up by deleting the created topics
            if created_topics:
                delete_result = admin_client.delete_topics(created_topics)
                logger.info(f"🧹 Deleting topics: {created_topics}")
                for topic_name, future in delete_result.items():
                    try:
                        future.result(timeout=10)
                        logger.info(f"✅ Deleted topic: {topic_name}")
                    except Exception as e:
                        logger.warning(
                            f"⚠️  Warning: Could not delete topic {topic_name}: {e}"
                        )
