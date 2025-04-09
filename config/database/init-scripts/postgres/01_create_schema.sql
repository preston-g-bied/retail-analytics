-- config/database/init-scripts/postgres/01_create_schema.sql
-- Initialize PostgreSQL schema for retail analytics

-- create schemas
CREATE SCHEMA IF NOT EXISTS retail;
CREATE SCHEMA IF NOT EXISTS staging;

-- create tables in retail schema

-- dimension tables
CREATE TABLE retail.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    customer_key VARCHAR(50), UNIQUE NOT NULL,  -- natural key from source system
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    updated at TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE retail.dim_product (
    product_id SERIAL PRIMARY KEY,
    product_key VARCHAR(50) UNIQUE NOT NULL,    -- natural key from source system
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    supplier VARCHAR(100),
    unit_price DECIMAL(10, 2) NOT NULL,
    cost_price DECIMAL(10, 2),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE retail.dim_location (
    location_id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    region VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE retail.dim_date (
    date_id INTEGER PRIMARY KEY,    -- YYYYMMDD format
    full_date DATE NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_year INTEGER NOT NULL,
    week_of_year INTEGER NOT NULL,
    month_number INTEGER NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    holiday_name VARCHAR(100)
);

-- fact tables
CREATE TABLE retail.fact_transaction (
    transaction_id SERIAL PRIMARY KEY,
    transaction_key VARCHAR(50) UNIQUE NOT NULL,    -- natural key from source system
    customer_id INTEGER REFERENCES retail.dim_customer(customer_id),
    date_id INTEGER REFERENCES retail.dim_date(date_id),
    location_id INTEGER REFERENCES retail.dim_location(location_id),
    total_amount DECIMAL(12, 2) NOT NULL,
    discount_amount DECIMAL(12, 2) DEFAULT 0,
    tax_amount DECIMAL(12, 2) DEFAULT 0,
    shipping_amount DECIMAL(12, 2) DEFAULT 0,
    payment_method VARCHAR(50),
    channel VARCHAR(50),
    transaction_time TIME,
    is_return BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE retail.fact_transaction_item (
    transaction_item_id SERIAL PRIMARY KEY,
    transaction_id INTEGER REFERENCES retail.fact_transaction(transaction_id),
    product_id INTEGER REFERENCES retail.dim_product(product_id),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    tax_amount DECIMAL(10, 2) DEFAULT 0,
    line_total DECIMAL(12, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE retail.fact_inventory (
    inventory_id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES retail.dim_product(product_id),
    date_id INTEGER REFERENCES retail.dim_date(date_id),
    location_id INTEGER REFERENCES retail.dim_location(location_id),
    quantity_on_hand INTEGER NOT NULL,
    quantity_reserved INTEGER DEFAULT 0,
    quantity_available INTEGER GENERATED ALWAYS AS (quantity_on_hand - quantity_reserved) STORED,
    reorder_point INTEGER,
    reorder_quantity INTEGER,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

-- create staging tables for ETL
CREATE TABLE staging.stg_customer (
    customer_key VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100),
    create_date TIMESTAMP,
    update_date TIMESTAMP,
    source VARCHAR(50),
    batch_id VARCHAR(50),
    processed_at TIMESTAMP
);

CREATE TABLE staging.stg_product (
    product_key VARCHAR(50) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    supplier VARCHAR(100),
    unit_price DECIMAL(10, 2),
    cost_price DECIMAL(10, 2),
    attributes JSONB,
    source VARCHAR(50),
    batch_id VARCHAR(50),
    processed_at TIMESTAMP
);

CREATE TABLE staging.stg_transaction (
    transaction_key VARCHAR(50) NOT NULL,
    transaction_date TIMESTAMP,
    customer_key VARCHAR(50),
    country VARCHAR(100),
    region VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    total_amount DECIMAL(12, 2),
    discount_amount DECIMAL(12, 2),
    tax_amount DECIMAL(12, 2),
    shipping_amount DECIMAL(12, 2),
    payment_method VARCHAR(50),
    channel VARCHAR(50),
    is_return BOOLEAN,
    source VARCHAR(50),
    batch_id VARCHAR(50),
    processed_at TIMESTAMP
);

CREATE TABLE staging.stg_transaction_item (
    transaction_key VARCHAR(50) NOT NULL,
    product_key VARCHAR(50) NOT NULL,
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    discount_amount DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    line_total DECIMAL(12, 2),
    source VARCHAR(50),
    batch_id VARCHAR(50),
    processed_at TIMESTAMP
);

-- create users and set permissions
CREATE USER etl_user WITH PASSWORD 'etl_password';
CREATE USER analytics_user WITH PASSWORD 'analytics_password';
CREATE USER app_user WITH PASSWORD 'app_password';

-- grant permissions to ETL user
GRANT USAGE ON SCHEMA retail TO etl_user;
GRANT USAGE ON SCHEMA stanging TO etl_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO etl_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO etl_user;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA retail TO etl_user;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA retaul TO etl_user

-- grant permissions to analytics user
GRANT USAGE ON SCHEMA retail TO analytics_user;
GRANT SELECT ALL TABLES IN SCHEMA retail TO analytics_user;

-- grant permissions to app user
GRANT USAGE ON SCHEMA retail TO app_user;
GRANT SELECT ON ALL TABLES IN SCHEMA retail TO app_user;

-- create indexes for performance
CREATE INDEX idx_fact_transaction_customer_id ON retail.fact_transaction(customer_id);
CREATE INDEX idx_fact_transaction_date_id ON retail.fact_transaction(date_id);
CREATE INDEX idx_fact_transaction_item_transaction_id ON retail.fact_transaction_item(transaction_id);
CREATE INDEX idx_fact_transaction_item_product_id ON retail.fact_transaction_item(product_id);
CREATE INDEX idx_fact_inventory_product_id ON retail.fact_inventory(product_id);
CREATE INDEX idx_fact_inventory_date_id ON retail.fact_inventory(date_id);

-- create a function to generate date dimension data
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO retail.dim_date (
            date_id,
            full_date,
            day_of_week,
            day_name,
            day_of_month,
            day_of_year,
            week_of_year,
            month_number,
            month_name,
            quarter,
            year,
            is_weekend,
            is_holiday,
            holiday_name
        ) VALUES (
            TO_CHAR(curr_date, 'YYYYMMDD')::INTEGER,
            curr_date,
            EXTRACT(DOW FROM curr_date),
            TO_CHAR(curr_date, 'Day'),
            EXTRACT(DAY FROM curr_date),
            EXTRACT(DOY FROM curr_date),
            EXTRACT(WEEK FROM curr_date),
            EXTRACT(MONTH FROM curr_date),
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(QUARTER FROM curr_date),
            EXTRACT(YEAR FROM curr_date),
            CASE WHEN EXTRACR(DOW FROM curr_date) IN (0, 6) THEN TRUE ELSE FALSE END,
            FALSE,  -- default is_holiday to FALSE; update specific holidays later
            NULL
        );

        curr_date := curr_date + 1;
    END LOOP;
END;
$$ LANGUAGE plpqsql;

-- populate date dimension for 3 years (adjust as needed)
SELECT populate_dim_date('2023-01-01', '2025-12-31');