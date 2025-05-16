WITH base AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        MIN(region) AS region,
        MIN(category) AS category,
        MIN(product) AS product,
        MIN(customer_type) AS customer_type,
        SUM(total_price) AS total_spent,
        COUNT(DISTINCT product) AS num_products,
        SUM(quantity) AS total_quantity,
        AVG(unit_price) AS avg_unit_price
    FROM {{ ref('ods_syn_l1_v_beverage_sales_data_limpio') }}
    GROUP BY order_id, customer_id, order_date
),

-- Días entre pedidos
pedidos_con_dias AS (
    SELECT
        *,
        LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date,
        DATEDIFF('day', LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date), order_date) AS days_between_orders
    FROM base
),

-- Métricas agregadas por cliente
metrics_per_customer AS (
    SELECT
        customer_id,
        AVG(total_spent) AS avg_spent_per_order,
        AVG(num_products) AS avg_products_per_order,
        AVG(CASE WHEN days_between_orders IS NOT NULL THEN days_between_orders END) AS avg_days_between_orders
    FROM pedidos_con_dias
    GROUP BY customer_id
),

-- Región con mayor gasto
top_region AS (
    SELECT customer_id, region AS top_region
    FROM (
        SELECT
            customer_id,
            region,
            region_spent,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY region_spent DESC) AS rn
        FROM (
            SELECT
                customer_id,
                region,
                SUM(total_spent) AS region_spent
            FROM base
            GROUP BY customer_id, region
        ) sub1
    ) sub2
    WHERE rn = 1
),

-- Categoría con mayor gasto
top_category AS (
    SELECT customer_id, category AS top_category
    FROM (
        SELECT
            customer_id,
            category,
            category_spent,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY category_spent DESC) AS rn
        FROM (
            SELECT
                customer_id,
                category,
                SUM(total_spent) AS category_spent
            FROM base
            GROUP BY customer_id, category
        ) sub1
    ) sub2
    WHERE rn = 1
),

-- Producto con mayor gasto
top_product AS (
    SELECT customer_id, product AS top_product
    FROM (
        SELECT
            customer_id,
            product,
            product_spent,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY product_spent DESC) AS rn
        FROM (
            SELECT
                customer_id,
                product,
                SUM(total_spent) AS product_spent
            FROM base
            GROUP BY customer_id, product
        ) sub1
    ) sub2
    WHERE rn = 1
),

-- Tipo de cliente más frecuente
dominant_type AS (
    SELECT customer_id, customer_type AS dominant_customer_type
    FROM (
        SELECT
            customer_id,
            customer_type,
            COUNT(*) AS freq,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
        FROM base
        GROUP BY customer_id, customer_type
    ) sub
    WHERE rn = 1
)

-- Unión final
SELECT
    m.customer_id,
    m.avg_spent_per_order,
    m.avg_products_per_order,
    m.avg_days_between_orders,
    r.top_region,
    c.top_category,
    p.top_product,
    d.dominant_customer_type
FROM metrics_per_customer m
LEFT JOIN top_region r ON m.customer_id = r.customer_id
LEFT JOIN top_category c ON m.customer_id = c.customer_id
LEFT JOIN top_product p ON m.customer_id = p.customer_id
LEFT JOIN dominant_type d ON m.customer_id = d.customer_id
