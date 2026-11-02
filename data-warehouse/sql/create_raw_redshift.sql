
-- DATA WAREHOUSE EIA
-- Preparacion de esquemas y tablas RAW para Redshift

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS marts;

-- MYSQL: CLIENTES
CREATE TABLE IF NOT EXISTS raw.mysql_customers (
    customer_id BIGINT,
    name VARCHAR(160),
    email VARCHAR(254),
    phone VARCHAR(32),
    city VARCHAR(100),
    department VARCHAR(100),
    registration_date DATE,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMPTZ,
    _source VARCHAR(30),
    _batch_id VARCHAR(100)
);

-- MYSQL: PRODUCTOS
CREATE TABLE IF NOT EXISTS raw.mysql_products (
    product_id BIGINT,
    name VARCHAR(180),
    category VARCHAR(80),
    price DECIMAL(12,2),
    description VARCHAR(500),
    is_active SMALLINT,
    updated_at TIMESTAMP,
    _loaded_at TIMESTAMPTZ,
    _source VARCHAR(30),
    _batch_id VARCHAR(100)
);

-- MYSQL: PEDIDOS
CREATE TABLE IF NOT EXISTS raw.mysql_orders (
    order_id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP,
    status VARCHAR(20),
    sales_channel VARCHAR(30),
    payment_method VARCHAR(30),
    total DECIMAL(14,2),
    _loaded_at TIMESTAMPTZ,
    _source VARCHAR(30),
    _batch_id VARCHAR(100)
);

-- MYSQL: DETALLES DE PEDIDOS
CREATE TABLE IF NOT EXISTS raw.mysql_order_items (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    subtotal DECIMAL(14,2),
    _loaded_at TIMESTAMPTZ,
    _source VARCHAR(30),
    _batch_id VARCHAR(100)
);

-- MONGODB: RESEÑAS
CREATE TABLE IF NOT EXISTS raw.mongo_reviews (
    review_id BIGINT,
    customer_id BIGINT,
    product_id BIGINT,
    rating INTEGER,
    reviewed_at TIMESTAMPTZ,
    document_json VARCHAR(MAX),
    _loaded_at TIMESTAMPTZ,
    _source VARCHAR(30),
    _batch_id VARCHAR(100)
);

-- MONGODB: EVENTOS DE NAVEGACION
CREATE TABLE IF NOT EXISTS raw.mongo_navigation_events (
    event_id BIGINT,
    event_type VARCHAR(100),
    occurred_at TIMESTAMPTZ,
    customer_id BIGINT,
    product_id BIGINT,
    sales_channel VARCHAR(100),
    document_json VARCHAR(MAX),
    _loaded_at TIMESTAMPTZ,
    _source VARCHAR(30),
    _batch_id VARCHAR(100)
);

