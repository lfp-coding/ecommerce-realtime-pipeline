"""
Unit tests for the Kafka data producer module.

Tests cover producer initialization, entity publishing, batch operations,
error handling, metrics tracking, and CLI functionality.
"""

import json
import uuid
from io import StringIO
from unittest.mock import Mock, call, patch

import pytest
from confluent_kafka import KafkaError, Message

from src.config.settings import Settings
from src.data_generator.producer import (
    DataProducer,
    ProducerMetrics,
    create_producer,
    run_cli,
)
from src.data_generator.schemas import Customer, Event, Order, OrderItem, Product


class TestProducerMetrics:
    """Test ProducerMetrics dataclass functionality."""

    def test_metrics_initialization(self):
        """Test that metrics are initialized with zero values."""
        metrics = ProducerMetrics()
        assert metrics.produced_messages == 0
        assert metrics.produced_bytes == 0
        assert metrics.errors == 0

    def test_metrics_record(self):
        """Test that metrics correctly record message production."""
        metrics = ProducerMetrics()

        # Record first message
        metrics.record(100)
        assert metrics.produced_messages == 1
        assert metrics.produced_bytes == 100
        assert metrics.errors == 0

        # Record second message
        metrics.record(250)
        assert metrics.produced_messages == 2
        assert metrics.produced_bytes == 350
        assert metrics.errors == 0

    def test_metrics_error_increment(self):
        """Test that error count can be incremented."""
        metrics = ProducerMetrics()
        metrics.errors += 1
        assert metrics.errors == 1


class TestDataProducer:
    """Test DataProducer class functionality."""

    @pytest.fixture
    def mock_settings(self):
        """Create a mock Settings object."""
        settings = Mock(spec=Settings)
        settings.KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
        settings.KAFKA_CLIENT_ID = "test-client"
        settings.KAFKA_PRODUCT_TOPIC = "products"
        settings.KAFKA_CUSTOMER_TOPIC = "customers"
        settings.KAFKA_ORDER_TOPIC = "orders"
        settings.KAFKA_EVENT_TOPIC = "events"
        settings.SERVICE_NAME = "test-service"
        settings.APP_ENV = "test"
        return settings

    @pytest.fixture
    def mock_producer(self):
        """Create a mock Kafka Producer."""
        return Mock()

    @patch("src.data_generator.producer.configure_logging")
    @patch("src.data_generator.producer.get_logger")
    @patch("src.data_generator.producer.KafkaProducer")
    @patch("src.data_generator.producer.atexit.register")
    def test_initialization(
        self,
        mock_atexit,
        mock_kafka_producer,
        mock_get_logger,
        mock_configure_logging,
        mock_settings,
    ):
        """Test DataProducer initialization."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        mock_logger.bind.return_value = mock_logger

        producer = DataProducer(mock_settings)

        # Verify logging configuration
        mock_configure_logging.assert_called_once_with(mock_settings)
        mock_get_logger.assert_called_once()
        mock_logger.bind.assert_called_once_with(component="producer")

        # Verify Kafka producer configuration
        expected_config = {
            "bootstrap.servers": "localhost:9092",
            "client.id": "test-client",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.num.messages": 10_000,
        }
        mock_kafka_producer.assert_called_once_with(expected_config)

        # Verify shutdown hook registration
        mock_atexit.assert_called_once_with(producer._shutdown_hook)

        # Verify metrics initialization
        assert isinstance(producer.metrics, ProducerMetrics)

    @patch("src.data_generator.producer.configure_logging")
    @patch("src.data_generator.producer.get_logger")
    @patch("src.data_generator.producer.KafkaProducer")
    @patch("src.data_generator.producer.atexit.register")
    def test_initialization_without_settings(
        self, mock_atexit, mock_kafka_producer, mock_get_logger, mock_configure_logging
    ):
        """Test DataProducer initialization without explicit settings."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        mock_logger.bind.return_value = mock_logger

        with patch("src.data_generator.producer.Settings") as mock_settings_class:
            mock_settings = Mock()
            mock_settings.KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
            mock_settings.KAFKA_CLIENT_ID = "ecommerce-app"
            mock_settings_class.return_value = mock_settings

            _ = DataProducer()

            # Verify Settings() was called
            mock_settings_class.assert_called_once()

    def _setup_producer_with_mocks(self, mock_settings):
        """Helper to set up a DataProducer with mocked dependencies."""
        with (
            patch("src.data_generator.producer.configure_logging"),
            patch("src.data_generator.producer.get_logger") as mock_get_logger,
            patch("src.data_generator.producer.KafkaProducer"),
            patch("src.data_generator.producer.atexit.register"),
        ):
            mock_logger = Mock()
            mock_get_logger.return_value = mock_logger
            mock_logger.bind.return_value = mock_logger

            producer = DataProducer(mock_settings)
            producer._producer = Mock()

            return producer

    def test_delivery_callback_success(self, mock_settings):
        """Test delivery callback for successful message delivery."""
        producer = self._setup_producer_with_mocks(mock_settings)

        # Mock successful message
        mock_message = Mock(spec=Message)
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 1
        mock_message.offset.return_value = 123

        # Call delivery callback with no error
        producer._delivery_callback(None, mock_message)

        # Verify no error count increment
        assert producer.metrics.errors == 0

    def test_delivery_callback_error(self, mock_settings):
        """Test delivery callback for failed message delivery."""
        producer = self._setup_producer_with_mocks(mock_settings)

        # Mock failed message
        mock_message = Mock(spec=Message)
        mock_message.topic.return_value = "test-topic"
        mock_message.partition.return_value = 1
        mock_message.offset.return_value = -1

        mock_error = Mock(spec=KafkaError)

        # Call delivery callback with error
        producer._delivery_callback(mock_error, mock_message)

        # Verify error count increment
        assert producer.metrics.errors == 1

    def test_produce_success(self, mock_settings):
        """Test successful message production."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._producer.poll = Mock()

        test_payload = b"test message"

        producer._produce(topic="test-topic", key="test-key", value=test_payload)

        # Verify producer.produce was called correctly
        producer._producer.produce.assert_called_once_with(
            topic="test-topic",
            key="test-key",
            value=test_payload,
            on_delivery=producer._delivery_callback,
        )

        # Verify metrics were updated
        assert producer.metrics.produced_messages == 1
        assert producer.metrics.produced_bytes == len(test_payload)

        # Verify poll was called
        producer._producer.poll.assert_called_once_with(0)

    def test_produce_buffer_error_retry(self, mock_settings):
        """Test retry mechanism for BufferError."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._producer.poll = Mock()

        # Mock BufferError on first call, success on second

        producer._producer.produce.side_effect = [BufferError("Queue full"), None]

        test_payload = b"test message"

        with patch("time.sleep") as mock_sleep:
            producer._produce(topic="test-topic", key="test-key", value=test_payload)

        # Verify retry mechanism
        assert producer._producer.produce.call_count == 2
        mock_sleep.assert_called_once_with(0.05)
        producer._producer.poll.assert_called()  # Called during backoff

    def test_produce_buffer_error_max_retries(self, mock_settings):
        """Test BufferError handling when max retries exceeded."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._producer.poll = Mock()

        producer._producer.produce.side_effect = BufferError("Queue full")

        test_payload = b"test message"

        with pytest.raises(BufferError), patch("time.sleep"):
            producer._produce(
                topic="test-topic", key="test-key", value=test_payload, max_retries=2
            )

        # Verify error count increment
        assert producer.metrics.errors == 1

    def test_produce_unexpected_error(self, mock_settings):
        """Test handling of unexpected errors during production."""
        producer = self._setup_producer_with_mocks(mock_settings)

        # Mock unexpected exception
        producer._producer.produce.side_effect = RuntimeError("Unexpected error")

        test_payload = b"test message"

        with pytest.raises(RuntimeError):
            producer._produce(topic="test-topic", key="test-key", value=test_payload)

        # Verify error count increment
        assert producer.metrics.errors == 1

    def test_produce_product(self, mock_settings):
        """Test product production."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._produce = Mock()

        product = Product(
            name="Test Product",
            category="electronics",
            price=99.99,
            description="A test product",
            stock_quantity=10,
        )

        producer.produce_product(product)

        producer._produce.assert_called_once_with(
            topic="products", key=str(product.product_id), value=product.to_json_bytes()
        )

    def test_produce_customer(self, mock_settings):
        """Test customer production."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._produce = Mock()

        customer = Customer(name="Test Customer", email="test@example.com")

        producer.produce_customer(customer)

        producer._produce.assert_called_once_with(
            topic="customers",
            key=str(customer.customer_id),
            value=customer.to_json_bytes(),
        )

    def test_produce_order(self, mock_settings):
        """Test order production with total computation."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._produce = Mock()

        order_item = OrderItem(
            product_id=uuid.UUID(
                "550e8400-e29b-41d4-a716-446655440000"
            ),  # UUID() constructor
            quantity=2,
            unit_price=50.0,
        )
        order = Order(
            customer_id=uuid.UUID("550e8400-e29b-41d4-a716-446655440001"),
            items=[order_item],
        )

        producer.produce_order(order)

        # Verify total was computed
        assert order.total == 100.0

        producer._produce.assert_called_once_with(
            topic="orders", key=str(order.order_id), value=order.to_json_bytes()
        )

    def test_produce_event(self, mock_settings):
        """Test event production."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._produce = Mock()

        event = Event(
            event_type="product_view",
            customer_id=uuid.UUID("550e8400-e29b-41d4-a716-446655440000"),
        )

        producer.produce_event(event)

        producer._produce.assert_called_once_with(
            topic="events", key=str(event.event_id), value=event.to_json_bytes()
        )

    @patch("src.data_generator.producer.utils")
    def test_produce_batch_with_all_entities(self, mock_utils, mock_settings):
        """Test batch production with all entity types."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer.produce_product = Mock()
        producer.produce_customer = Mock()
        producer.produce_order = Mock()
        producer.produce_event = Mock()
        producer.flush = Mock()

        # Mock utils functions
        mock_products = [Mock(), Mock()]
        mock_customers = [Mock(), Mock()]
        mock_orders = [Mock()]
        mock_events = [Mock(), Mock(), Mock()]

        mock_customers[0].customer_id = "cust1"
        mock_customers[1].customer_id = "cust2"

        mock_utils.set_random_seed = Mock()
        mock_utils.generate_products.return_value = mock_products
        mock_utils.generate_customers.return_value = mock_customers
        mock_utils.generate_orders.return_value = mock_orders
        mock_utils.generate_events.return_value = mock_events

        result = producer.produce_batch(
            product_count=2,
            customer_count=2,
            order_count=1,
            event_count=3,
            seed=42,
            flush=True,
        )

        # Verify utils calls
        mock_utils.set_random_seed.assert_called_once_with(42)
        mock_utils.generate_products.assert_called_once_with(2)
        mock_utils.generate_customers.assert_called_once_with(2)
        mock_utils.generate_orders.assert_called_once_with(
            1, products=mock_products, customer_ids=["cust1", "cust2"]
        )
        mock_utils.generate_events.assert_called_once_with(
            3, customer_ids=["cust1", "cust2"]
        )

        # Verify entity production calls
        assert producer.produce_product.call_count == 2
        assert producer.produce_customer.call_count == 2
        assert producer.produce_order.call_count == 1
        assert producer.produce_event.call_count == 3

        # Verify flush was called
        producer.flush.assert_called_once()

        # Verify return value
        assert result == producer.metrics

    @patch("src.data_generator.producer.utils")
    def test_produce_batch_no_flush(self, mock_utils, mock_settings):
        """Test batch production without flush."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer.flush = Mock()

        # Mock empty generation
        mock_utils.set_random_seed = Mock()
        mock_utils.generate_products.return_value = []
        mock_utils.generate_customers.return_value = []

        producer.produce_batch(product_count=0, customer_count=0, flush=False)

        # Verify flush was not called
        producer.flush.assert_not_called()

    def test_flush(self, mock_settings):
        """Test producer flush functionality."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._producer.flush.return_value = 0  # No remaining messages

        producer.flush(timeout=5.0)

        producer._producer.flush.assert_called_once_with(timeout=5.0)

    def test_flush_with_remaining_messages(self, mock_settings):
        """Test flush with remaining messages."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer._producer.flush.return_value = 5  # 5 remaining messages

        producer.flush()

        # Should log warning about remaining messages

    def test_close(self, mock_settings):
        """Test producer close functionality."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer.flush = Mock()

        producer.close()

        producer.flush.assert_called_once()

    def test_shutdown_hook(self, mock_settings):
        """Test shutdown hook handles exceptions gracefully."""
        producer = self._setup_producer_with_mocks(mock_settings)
        producer.close = Mock(side_effect=Exception("Test exception"))

        # Should not raise exception
        producer._shutdown_hook()


class TestFactoryAndCLI:
    """Test factory function and CLI interface."""

    @patch("src.data_generator.producer.DataProducer")
    @patch("src.data_generator.producer.Settings")
    def test_create_producer(self, mock_settings_class, mock_data_producer_class):
        """Test producer factory function."""
        mock_settings = Mock()
        mock_producer = Mock()
        mock_settings_class.return_value = mock_settings
        mock_data_producer_class.return_value = mock_producer

        result = create_producer()

        mock_settings_class.assert_called_once()
        mock_data_producer_class.assert_called_once_with(mock_settings)
        assert result == mock_producer

    @patch("src.data_generator.producer.create_producer")
    def test_run_cli_default_args(self, mock_create_producer):
        """Test CLI with default arguments."""
        mock_producer = Mock()
        mock_create_producer.return_value = mock_producer

        mock_metrics = ProducerMetrics()
        mock_metrics.produced_messages = 40
        mock_metrics.produced_bytes = 1000
        mock_metrics.errors = 0
        mock_producer.metrics = mock_metrics

        # Capture stdout
        captured_output = StringIO()

        with patch("sys.stdout", captured_output):
            result = run_cli([])

        # Verify producer calls
        mock_producer.produce_batch.assert_called_once_with(
            product_count=5,
            customer_count=5,
            order_count=10,
            event_count=25,
            seed=None,
            flush=True,
        )

        # Verify return code
        assert result == 0

        # Verify JSON output
        output = captured_output.getvalue()
        output_data = json.loads(output)
        assert output_data["produced_messages"] == 40
        assert output_data["produced_bytes"] == 1000
        assert output_data["errors"] == 0

    @patch("src.data_generator.producer.create_producer")
    def test_run_cli_custom_args(self, mock_create_producer):
        """Test CLI with custom arguments."""
        mock_producer = Mock()
        mock_create_producer.return_value = mock_producer

        mock_metrics = ProducerMetrics()
        mock_producer.metrics = mock_metrics

        with patch("sys.stdout", StringIO()):
            run_cli(
                [
                    "--products",
                    "10",
                    "--customers",
                    "15",
                    "--orders",
                    "20",
                    "--events",
                    "50",
                    "--seed",
                    "123",
                    "--no-flush",
                ]
            )

        mock_producer.produce_batch.assert_called_once_with(
            product_count=10,
            customer_count=15,
            order_count=20,
            event_count=50,
            seed=123,
            flush=False,
        )

    @patch("src.data_generator.producer.create_producer")
    @patch("time.sleep")
    def test_run_cli_repeat(self, mock_sleep, mock_create_producer):
        """Test CLI with repeat functionality."""
        mock_producer = Mock()
        mock_create_producer.return_value = mock_producer

        mock_metrics = ProducerMetrics()
        mock_producer.metrics = mock_metrics

        with patch("sys.stdout", StringIO()):
            run_cli(["--repeat", "3", "--sleep", "0.1", "--products", "1"])

        # Verify repeated calls
        assert mock_producer.produce_batch.call_count == 3

        # Verify sleep calls (called between batches, so 2 times)
        assert mock_sleep.call_count == 2
        mock_sleep.assert_has_calls([call(0.1), call(0.1)])

    @patch("src.data_generator.producer.create_producer")
    def test_run_cli_with_errors(self, mock_create_producer):
        """Test CLI return code when errors occur."""
        mock_producer = Mock()
        mock_create_producer.return_value = mock_producer

        mock_metrics = ProducerMetrics()
        mock_metrics.errors = 5  # Simulate errors
        mock_producer.metrics = mock_metrics

        with patch("sys.stdout", StringIO()):
            result = run_cli([])

        # Verify non-zero return code
        assert result == 1

    @patch("src.data_generator.producer.run_cli")
    def test_main_function(self, mock_run_cli):
        """Test main function calls run_cli and exits."""
        mock_run_cli.return_value = 42

        with pytest.raises(SystemExit) as exc_info:
            from src.data_generator.producer import main

            main()

        assert exc_info.value.code == 42
        mock_run_cli.assert_called_once()

    def test_cli_argument_parser(self):
        """Test argument parser configuration."""
        from src.data_generator.producer import run_cli

        # Test with --help to see if parser is configured correctly
        with (
            patch("sys.argv", ["producer.py", "--help"]),
            patch("sys.stdout", StringIO()),
            pytest.raises(SystemExit),
        ):
            run_cli(["--help"])
