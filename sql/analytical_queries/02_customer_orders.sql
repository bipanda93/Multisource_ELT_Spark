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
    COALESCE(
        c.email,
        'Inconnu'
    ) AS email,
    COUNT(DISTINCT o.order_id) AS number_of_orders,
    COALESCE(
        SUM(o.total_amount),
        0
    ) AS total_spent
FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id
GROUP BY
    o.customer_id,
    c.first_name,
    c.last_name,
    c.email
ORDER BY total_spent DESC;