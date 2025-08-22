# Use application settings for DB connection
import psycopg2
import pytest

from src.config.settings import Settings


def get_test_db_connection():
    """
    Get a connection to the test database using Settings.
    """
    settings = Settings()
    # Require test DB name for safety
    dbname = getattr(settings, "DB_TEST_NAME", None)
    if not dbname:
        raise RuntimeError(
            "DB_TEST_NAME is not set in environment or settings. Integration tests must use a dedicated test database."
        )
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=dbname,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


# Test if schemas exist
def test_schemas_exist():
    """
    Test that required schemas are created in the test database.
    """
    required_schemas = ["raw_data", "analytics", "staging", "marts", "monitoring"]
    with get_test_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT schema_name FROM information_schema.schemata;")
            schemas = {row[0] for row in cur.fetchall()}
    for schema in required_schemas:
        assert schema in schemas, f"Schema '{schema}' not found in test DB."


# Test if tables exist in each schema
@pytest.mark.parametrize(
    "schema, tables",
    [
        ("raw_data", ["products_raw", "customers_raw", "orders_raw", "events_raw"]),
        ("analytics", ["products", "customers", "orders", "order_items", "events"]),
        ("monitoring", ["consumer_offsets", "processing_stats"]),
    ],
)
def test_tables_exist(schema, tables):
    """
    Test that required tables are created in each schema.
    """
    with get_test_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s;
            """,
                (schema,),
            )
            found_tables = {row[0] for row in cur.fetchall()}
    for table in tables:
        assert table in found_tables, f"Table '{table}' not found in schema '{schema}'."


# Test if all indexes exist
INDEX_DEFINITIONS = [
    # raw_data indexes
    (
        "raw_data",
        "products_raw",
        [
            "idx_products_raw_consumed_at",
            "idx_products_raw_offset",
            "idx_products_raw_payload_gin",
        ],
    ),
    (
        "raw_data",
        "customers_raw",
        [
            "idx_customers_raw_consumed_at",
            "idx_customers_raw_offset",
            "idx_customers_raw_payload_gin",
        ],
    ),
    (
        "raw_data",
        "orders_raw",
        [
            "idx_orders_raw_consumed_at",
            "idx_orders_raw_offset",
            "idx_orders_raw_payload_gin",
        ],
    ),
    ("raw_data", "events_raw", ["idx_events_raw_consumed_at", "idx_events_raw_offset"]),
    # analytics indexes
    (
        "analytics",
        "products",
        ["idx_products_category", "idx_products_price", "idx_products_created_at"],
    ),
    ("analytics", "customers", ["idx_customers_email", "idx_customers_created_at"]),
    (
        "analytics",
        "orders",
        [
            "idx_orders_customer_id",
            "idx_orders_status",
            "idx_orders_created_at",
            "idx_orders_total",
            "idx_orders_customer_created",
            "idx_orders_status_created",
            "idx_orders_active_status",
        ],
    ),
    (
        "analytics",
        "order_items",
        ["idx_order_items_order_id", "idx_order_items_product_id"],
    ),
    (
        "analytics",
        "events",
        [
            "idx_events_customer_id",
            "idx_events_type",
            "idx_events_timestamp",
            "idx_events_customer_timestamp",
            "idx_events_type_timestamp",
        ],
    ),
    # monitoring indexes
    ("monitoring", "consumer_offsets", ["idx_consumer_offsets_topic"]),
    ("monitoring", "processing_stats", ["idx_processing_stats_table_timestamp"]),
]


@pytest.mark.parametrize("schema, table, index_names", INDEX_DEFINITIONS)
def test_all_indexes_exist(schema, table, index_names):
    """
    Test that all indexes from 03_create_indexes.sql exist on the correct tables.
    """
    with get_test_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE schemaname = %s AND tablename = %s;
                """,
                (schema, table),
            )
            found_indexes = {row[0] for row in cur.fetchall()}
    for idx in index_names:
        assert idx in found_indexes, (
            f"Index '{idx}' not found on table '{schema}.{table}'."
        )
