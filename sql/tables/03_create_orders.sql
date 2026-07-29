CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    order_date DATE,
    status VARCHAR(50),
    payment_method VARCHAR(50),
    total_amount DECIMAL(10, 2),
    currency VARCHAR(10)
);