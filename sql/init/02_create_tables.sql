-- ==============================================================================
-- E-COMMERCE ANALYTICS DATABASE - TABLE DEFINITIONS
-- Creates tables matching the Pydantic schemas from data_generator/schemas.py
-- ==============================================================================

-- Raw data tables (direct from Kafka topics)
-- These store the exact JSON payloads for audit/reprocessing

CREATE TABLE IF NOT EXISTS raw_data.products_raw (
    id SERIAL PRIMARY KEY,
    topic_partition INTEGER,
    topic_offset BIGINT,
    message_key TEXT,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(topic_partition, topic_offset)
);

CREATE TABLE IF NOT EXISTS raw_data.customers_raw (
    id SERIAL PRIMARY KEY,
    topic_partition INTEGER,
    topic_offset BIGINT,
    message_key TEXT,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(topic_partition, topic_offset)
);

CREATE TABLE IF NOT EXISTS raw_data.orders_raw (
    id SERIAL PRIMARY KEY,
    topic_partition INTEGER,
    topic_offset BIGINT,
    message_key TEXT,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(topic_partition, topic_offset)
);

CREATE TABLE IF NOT EXISTS raw_data.events_raw (
    id SERIAL PRIMARY KEY,
    topic_partition INTEGER,
    topic_offset BIGINT,
    message_key TEXT,
    payload JSONB NOT NULL,
    consumed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(topic_partition, topic_offset)
);

-- Normalized business tables (analytics schema)
-- These match your Pydantic models exactly

CREATE TABLE IF NOT EXISTS analytics.products (
    product_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    price DECIMAL(10,2) NOT NULL CHECK (price >= 0),
    description TEXT,
    stock_quantity INTEGER NOT NULL DEFAULT 0 CHECK (stock_quantity >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Audit fields
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.customers (
    customer_id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Audit fields
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    total DECIMAL(10,2) CHECK (total >= 0),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled')),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Audit fields
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_modified TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Foreign keys
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id) REFERENCES analytics.customers(customer_id)
);

CREATE TABLE IF NOT EXISTS analytics.order_items (
    id SERIAL PRIMARY KEY,
    order_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price DECIMAL(10,2) NOT NULL CHECK (unit_price >= 0),
    line_total DECIMAL(10,2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    -- Audit fields
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Foreign keys
    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id) REFERENCES analytics.orders(order_id) ON DELETE CASCADE,
    CONSTRAINT fk_order_items_product
        FOREIGN KEY (product_id) REFERENCES analytics.products(product_id),
    -- Business constraints
    UNIQUE(order_id, product_id) -- Prevent duplicate products per order
);

CREATE TABLE IF NOT EXISTS analytics.events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('page_view', 'product_view', 'add_to_cart', 'remove_from_cart', 'purchase')),
    customer_id UUID NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    -- Audit fields
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    -- Foreign keys
    CONSTRAINT fk_events_customer
        FOREIGN KEY (customer_id) REFERENCES analytics.customers(customer_id)
);

-- Monitoring tables for pipeline health
CREATE TABLE IF NOT EXISTS monitoring.consumer_offsets (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_committed BIGINT NOT NULL,
    consumer_group TEXT NOT NULL,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (topic, partition_id, consumer_group)
);

CREATE TABLE IF NOT EXISTS monitoring.processing_stats (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    records_processed INTEGER NOT NULL DEFAULT 0,
    records_failed INTEGER NOT NULL DEFAULT 0,
    batch_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processing_duration_ms INTEGER
);

-- Create trigger to auto-update last_modified timestamp
CREATE OR REPLACE FUNCTION update_last_modified()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_modified = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply auto-update triggers to business tables
CREATE TRIGGER products_update_modified
    BEFORE UPDATE ON analytics.products
    FOR EACH ROW EXECUTE FUNCTION update_last_modified();

CREATE TRIGGER customers_update_modified
    BEFORE UPDATE ON analytics.customers
    FOR EACH ROW EXECUTE FUNCTION update_last_modified();

CREATE TRIGGER orders_update_modified
    BEFORE UPDATE ON analytics.orders
    FOR EACH ROW EXECUTE FUNCTION update_last_modified();
