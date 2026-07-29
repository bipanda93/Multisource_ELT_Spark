CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    discount DECIMAL(5, 2),

    PRIMARY KEY (order_id, product_id)
);