WITH pedidos_por_cliente AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        COUNT(DISTINCT product) AS num_products,
        SUM(quantity) AS total_quantity,
        SUM(total_price) AS total_spent,
        AVG(unit_price) AS avg_price_per_product
    FROM {{ ref('ods_syn_l1_v_beverage_sales_data_limpio') }}
    GROUP BY order_id, customer_id, order_date
)

SELECT
    *,
    LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date,
    DATEDIFF('day', LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date), order_date) AS days_since_last_order
FROM pedidos_por_cliente
