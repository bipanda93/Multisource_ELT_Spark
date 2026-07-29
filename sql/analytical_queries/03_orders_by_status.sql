SELECT
    status,
    COUNT(*) AS number_of_orders,
    SUM(total_amount) AS total_amount,
    ROUND(
        AVG(total_amount),
        2
    ) AS average_amount
FROM orders
GROUP BY status
ORDER BY number_of_orders DESC;