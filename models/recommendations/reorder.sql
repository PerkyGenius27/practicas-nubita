WITH productos_cliente AS (
    SELECT
        customer_id,
        product,
        SUM(total_quantity) AS total_quantity
    FROM {{ ref('ods_cli_l6_v_beverage_sales_data_categorizados') }}
    GROUP BY customer_id, product
),

top_comprados AS (
    SELECT
        customer_id,
        product,
        total_quantity,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_quantity DESC) AS ranking
    FROM productos_cliente
)

SELECT
    customer_id,
    product,
    total_quantity AS score,
    ranking,
    'reorder' AS recommendation_type
FROM top_comprados
WHERE ranking <= 10
