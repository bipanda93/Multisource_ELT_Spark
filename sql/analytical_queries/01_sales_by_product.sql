SELECT
    oi.product_id,
    COALESCE(
        p.product_name,
        'Produit supprimé'
    ) AS product_name,
    COALESCE(
        p.category,
        'Inconnue'
    ) AS category,
    COALESCE(
        p.brand,
        'Inconnue'
    ) AS brand,
    SUM(oi.quantity) AS quantity_sold,
    ROUND(
        SUM(
            oi.quantity
            * oi.unit_price
            * (1 - oi.discount)
        ),
        2
    ) AS revenue
FROM order_items oi
LEFT JOIN products p
    ON oi.product_id = p.product_id
GROUP BY
    oi.product_id,
    p.product_name,
    p.category,
    p.brand
ORDER BY revenue DESC;