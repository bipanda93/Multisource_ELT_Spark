CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(30),
    city VARCHAR(100),
    country VARCHAR(100),
    birth_date DATE,
    created_at TIMESTAMP
);