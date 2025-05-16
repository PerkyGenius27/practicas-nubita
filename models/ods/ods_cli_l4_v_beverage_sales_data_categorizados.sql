WITH pedidos_por_cliente AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        MIN(region) AS region,
        MIN(customer_type) AS customer_type,
        MIN(category) AS category,
        MIN(product) AS product,
        COUNT(DISTINCT product) AS num_products,
        SUM(quantity) AS total_quantity,
        SUM(total_price) AS total_spent,
        AVG(unit_price) AS avg_price_per_product
    FROM {{ ref('ods_syn_l1_v_beverage_sales_data_limpio') }}
    GROUP BY order_id, customer_id, order_date
),

pedidos_enriquecidos AS (
    SELECT
        *,
        -- Orden anterior
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date,
        DATEDIFF('day', LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date), order_date) AS days_since_last_order,

        -- Agregados
        AVG(total_spent) OVER (PARTITION BY customer_id) AS avg_spent_per_order,
        AVG(num_products) OVER (PARTITION BY customer_id) AS avg_products_per_order,

        AVG(total_spent) OVER (PARTITION BY region) AS avg_spent_per_region,
        AVG(total_spent) OVER (PARTITION BY customer_type) AS avg_spent_per_customer_type,
        AVG(total_spent) OVER (PARTITION BY category) AS avg_spent_per_category,
        AVG(total_spent) OVER (PARTITION BY product) AS avg_spent_per_product
    FROM pedidos_por_cliente
)

SELECT *
FROM pedidos_enriquecidos
