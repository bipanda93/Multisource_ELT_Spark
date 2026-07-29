WITH parsed_orders AS (
    SELECT
        CASE
            WHEN TRIM(order_date) ~ '^\d{2}/\d{2}/\d{4}$'
            THEN TO_DATE(
                TRIM(order_date),
                'DD/MM/YYYY'
            )

            WHEN TRIM(order_date) ~ '^\d{4}-\d{2}-\d{2}$'
            THEN TO_DATE(
                TRIM(order_date),
                'YYYY-MM-DD'
            )

            WHEN TRIM(order_date)
                ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}$'
            THEN TO_TIMESTAMP(
                REPLACE(
                    TRIM(order_date),
                    'T',
                    ' '
                ),
                'YYYY-MM-DD HH24:MI:SS'
            )::date

            ELSE NULL
        END AS parsed_order_date,

        total_amount,
        status

    FROM orders
)

SELECT
    DATE_TRUNC(
        'month',
        parsed_order_date
    ) AS order_month,

    COUNT(*) AS number_of_orders,

    COALESCE(
        SUM(total_amount),
        0
    ) AS monthly_revenue,

    ROUND(
        COALESCE(
            AVG(total_amount),
            0
        ),
        2
    ) AS average_order_amount

FROM parsed_orders

WHERE
    parsed_order_date IS NOT NULL

    AND COALESCE(
        UPPER(TRIM(status)),
        'UNKNOWN'
    ) NOT IN (
        'CANCELLED',
        'ANNULÉE',
        'ANNULEE'
    )

GROUP BY
    DATE_TRUNC(
        'month',
        parsed_order_date
    )

ORDER BY
    order_month;