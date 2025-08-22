-- ==============================================================================
-- E-COMMERCE ANALYTICS DATABASE - INDEX OPTIMIZATION
-- Performance indexes for query patterns and foreign key relationships
-- ==============================================================================

-- Raw data indexes for efficient querying
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_raw_consumed_at
    ON raw_data.products_raw(consumed_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_raw_consumed_at
    ON raw_data.customers_raw(consumed_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_raw_consumed_at
    ON raw_data.orders_raw(consumed_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_raw_consumed_at
    ON raw_data.events_raw(consumed_at);

-- Kafka offset tracking (for idempotency)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_raw_offset
    ON raw_data.products_raw(topic_partition, topic_offset);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_raw_offset
    ON raw_data.customers_raw(topic_partition, topic_offset);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_raw_offset
    ON raw_data.orders_raw(topic_partition, topic_offset);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_raw_offset
    ON raw_data.events_raw(topic_partition, topic_offset);

-- Analytics tables - Business query patterns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_category
    ON analytics.products(category);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_price
    ON analytics.products(price);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_created_at
    ON analytics.products(created_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_email
    ON analytics.customers(email); -- Column is already unique, but a non-unique index can improve lookup speed for analytical queries and avoid locking issues during concurrent inserts/updates.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_created_at
    ON analytics.customers(created_at);

-- Orders - Critical for analytics queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_customer_id
    ON analytics.orders(customer_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_status
    ON analytics.orders(status);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_created_at
    ON analytics.orders(created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_total
    ON analytics.orders(total);

-- Composite index for time-series analytics (common query pattern)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_customer_created
    ON analytics.orders(customer_id, created_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_status_created
    ON analytics.orders(status, created_at);

-- Order items - Foreign key performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_items_order_id
    ON analytics.order_items(order_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_order_items_product_id
    ON analytics.order_items(product_id);

-- Events - High-volume table
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_customer_id
    ON analytics.events(customer_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_type
    ON analytics.events(event_type);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_timestamp
    ON analytics.events(timestamp);

-- Composite indexes for funnel analysis
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_customer_timestamp
    ON analytics.events(customer_id, timestamp);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_type_timestamp
    ON analytics.events(event_type, timestamp);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_active_status
    ON analytics.orders(created_at, customer_id)
    WHERE status IN ('pending', 'processing');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_consumer_offsets_topic
    ON monitoring.consumer_offsets(topic, partition_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_processing_stats_table_timestamp
    ON monitoring.processing_stats(table_name, batch_timestamp);
-- GIN index for efficient JSONB containment and key/value queries on products_raw payload
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_raw_payload_gin
    ON raw_data.products_raw USING GIN(payload);

-- GIN index for fast JSONB attribute filtering and analytics on customers_raw payload
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_customers_raw_payload_gin
    ON raw_data.customers_raw USING GIN(payload);

-- GIN index to optimize queries extracting fields or filtering on orders_raw payload JSONB
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_orders_raw_payload_gin
    ON raw_data.orders_raw USING GIN(payload);

-- Update statistics for query planner optimization
-- NOTE: In production, run ANALYZE after bulk data loads, major schema changes, or periodically (e.g., nightly) to ensure optimal query performance.
ANALYZE raw_data.products_raw;
ANALYZE raw_data.customers_raw;
ANALYZE raw_data.orders_raw;
ANALYZE raw_data.events_raw;
ANALYZE analytics.products;
ANALYZE analytics.customers;
ANALYZE analytics.orders;
ANALYZE raw_data.products_raw;
ANALYZE raw_data.customers_raw;
ANALYZE raw_data.orders_raw;
ANALYZE raw_data.events_raw;
ANALYZE analytics.products;
ANALYZE analytics.customers;
ANALYZE analytics.orders;
ANALYZE analytics.order_items;
ANALYZE analytics.events;
