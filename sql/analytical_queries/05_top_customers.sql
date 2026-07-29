SELECT
    o.customer_id,
    COALESCE(
        CONCAT(
            c.first_name,
            ' ',
            c.last_name
        ),
        'Client inconnu'
    ) AS full_name,
    COUNT(DISTINCT o.order_id) AS number_of_orders,
    SUM(o.total_amount) AS total_spent
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
GROUP BY
    o.customer_id,
    c.first_name,
    c.last_name
ORDER BY total_spent DESC
LIMIT 10;