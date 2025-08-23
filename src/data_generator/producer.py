"""Kafka data producer for synthetic e-commerce domain objects.

Features
--------
* Idempotent, safe Kafka producer configuration (acks=all, enable.idempotence)
* Typed helper methods to publish Product / Customer / Order / Event objects
* Batch generation convenience (generate + publish in one call)
* Delivery report logging (success + error) with lightweight retry/backoff
* Graceful shutdown & flush on exit
* CLI interface for ad‑hoc batch production

Example
-------
        python -m data_generator.producer --products 25 --customers 10 --orders 50 --events 120 --seed 42

"""

import argparse
import atexit
import json
import time
from dataclasses import dataclass

from confluent_kafka import KafkaError, Message
from confluent_kafka import Producer as KafkaProducer

from src.config.logging_config import configure_logging, get_logger
from src.config.settings import Settings

from . import utils
from .schemas import Customer, Event, Order, Product

__all__ = [
    "DataProducer",
    "ProducerMetrics",
    "create_producer",
    "run_cli",
]


@dataclass(slots=True)
class ProducerMetrics:
    """Simple in‑memory counters (expandable or exportable later)."""

    produced_messages: int = 0
    produced_bytes: int = 0
    errors: int = 0

    def record(self, payload_size: int) -> None:
        self.produced_messages += 1
        self.produced_bytes += payload_size


class DataProducer:
    """High-level domain producer wrapping a confluent_kafka `Producer`.

    The class exposes entity‑specific publish methods plus a combined
    `produce_batch` generator that uses the synthetic data helpers.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()

        # Logging setup just once per process (idempotent if already configured)
        configure_logging(self.settings)
        self.log = get_logger(__name__, self.settings).bind(component="producer")

        kafka_conf = {
            "bootstrap.servers": self.settings.KAFKA_BOOTSTRAP_SERVERS,
            "client.id": self.settings.KAFKA_CLIENT_ID,
            "enable.idempotence": True,  # exactly-once semantics (broker permitting)
            "acks": "all",
            "compression.type": "lz4",
            # Reasonable batching defaults; can be tuned.
            "linger.ms": 5,
            "batch.num.messages": 10_000,
        }
        self._producer = KafkaProducer(kafka_conf)
        self.metrics = ProducerMetrics()

        atexit.register(self._shutdown_hook)
        self.log.info("producer.initialized", config=kafka_conf)

    # ------------------------------------------------------------------
    # Core low-level produce
    # ------------------------------------------------------------------
    def _delivery_callback(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None:
            self.metrics.errors += 1
            self.log.error(
                "producer.delivery_failed",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                error=str(err),
            )
        else:
            self.log.debug(
                "producer.delivery_ok",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )

    def _produce(
        self, *, topic: str, key: str | None, value: bytes, max_retries: int = 3
    ) -> None:
        """Internal wrapper adding retry/backoff and metrics tracking.

        Backoff strategy: simple fixed sleep (50ms) for transient queue full.
        """

        attempt = 0
        while True:
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=self._delivery_callback,
                )
                self.metrics.record(len(value))
                break
            except BufferError as e:  # local queue full
                attempt += 1
                if attempt > max_retries:
                    self.metrics.errors += 1
                    self.log.error(
                        "producer.local_queue_full", topic=topic, error=str(e)
                    )
                    raise
                self.log.warning(
                    "producer.backpressure",
                    attempt=attempt,
                    topic=topic,
                    action="poll+sleep",
                )
                self._producer.poll(0)  # serve delivery callbacks -> free queue
                time.sleep(0.05)  # small backoff
            except Exception as e:
                self.metrics.errors += 1
                self.log.exception(
                    "producer.unexpected_error", topic=topic, error=str(e)
                )
                raise

        # Serve callbacks opportunistically (non-blocking)
        self._producer.poll(0)

    # ------------------------------------------------------------------
    # Entity specific helpers
    # ------------------------------------------------------------------
    def produce_product(self, product: Product) -> None:
        self._produce(
            topic=self.settings.KAFKA_PRODUCT_TOPIC,
            key=str(product.product_id),
            value=product.to_json_bytes(),
        )

    def produce_customer(self, customer: Customer) -> None:
        self._produce(
            topic=self.settings.KAFKA_CUSTOMER_TOPIC,
            key=str(customer.customer_id),
            value=customer.to_json_bytes(),
        )

    def produce_order(self, order: Order) -> None:
        # Ensure total computed for downstream consumers.
        order.compute_total()
        self._produce(
            topic=self.settings.KAFKA_ORDER_TOPIC,
            key=str(order.order_id),
            value=order.to_json_bytes(),
        )

    def produce_event(self, event: Event) -> None:
        self._produce(
            topic=self.settings.KAFKA_EVENT_TOPIC,
            key=str(event.event_id),
            value=event.to_json_bytes(),
        )

    # ------------------------------------------------------------------
    # Batch convenience
    # ------------------------------------------------------------------
    def produce_batch(
        self,
        *,
        product_count: int = 0,
        customer_count: int = 0,
        order_count: int = 0,
        event_count: int = 0,
        seed: int | None = None,
        flush: bool = True,
        corruption_enabled: bool | None = None,
    ) -> ProducerMetrics:
        """Generate & publish a batch of synthetic records using utils.generate_batch."""

        if corruption_enabled is None:
            corruption_enabled = bool(
                getattr(self.settings, "CORRUPTION_ENABLED", True)
            )

        batch = utils.generate_batch(
            product_count=product_count,
            customer_count=customer_count,
            order_count=order_count,
            event_count=event_count,
            seed=seed,
            corruption_enabled=corruption_enabled,
            settings=self.settings,
        )

        for p in batch.products:
            self.produce_product(p)
        for c in batch.customers:
            self.produce_customer(c)
        for o in batch.orders:
            self.produce_order(o)
        for e in batch.events:
            self.produce_event(e)

        if flush:
            self.flush()

        self.log.info(
            "producer.batch_complete",
            products=len(batch.products),
            customers=len(batch.customers),
            orders=len(batch.orders),
            events=len(batch.events),
            produced_messages=self.metrics.produced_messages,
            errors=self.metrics.errors,
        )
        return self.metrics

    # ------------------------------------------------------------------
    # Flush & shutdown
    # ------------------------------------------------------------------
    def flush(self, timeout: float = 10.0) -> None:
        remaining = self._producer.flush(timeout=timeout)
        if remaining > 0:
            self.log.warning("producer.flush_incomplete", remaining=remaining)

    def close(self) -> None:
        self.flush()

    def _shutdown_hook(self) -> None:
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass


# ----------------------------------------------------------------------
# Factory / CLI
# ----------------------------------------------------------------------
def create_producer(use_env_file: bool = False, env_file: str = ".env") -> DataProducer:
    if use_env_file:
        settings = Settings.from_env_file(env_file)
    else:
        settings = Settings()
    return DataProducer(settings)


def run_cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Produce a synthetic batch to Kafka")
    parser.add_argument("--products", type=int, default=5)
    parser.add_argument("--customers", type=int, default=5)
    parser.add_argument("--orders", type=int, default=10)
    parser.add_argument("--events", type=int, default=25)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--no-flush",
        action="store_true",
        help="Return immediately without final flush (faster, less safe)",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=1,
        help="Repeat the batch N times (metrics accumulate).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Seconds to sleep between repeated batches.",
    )
    args = parser.parse_args(argv)

    producer = create_producer()
    for i in range(args.repeat):
        if i > 0 and args.sleep > 0:
            time.sleep(args.sleep)
        producer.produce_batch(
            product_count=args.products,
            customer_count=args.customers,
            order_count=args.orders,
            event_count=args.events,
            seed=args.seed,
            flush=not args.no_flush,
        )

    metrics = producer.metrics
    print(  # stdout summary (structlog already emitted detailed logs)
        json.dumps(
            {
                "produced_messages": metrics.produced_messages,
                "produced_bytes": metrics.produced_bytes,
                "errors": metrics.errors,
            },
            indent=2,
        )
    )
    return 0 if metrics.errors == 0 else 1


def main() -> None:
    raise SystemExit(run_cli())


if __name__ == "__main__":
    main()
