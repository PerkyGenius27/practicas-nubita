WITH productos_cliente AS (
    SELECT
        customer_id,
        product,
        SUM(total_quantity) AS total_quantity
    FROM {{ ref('ods_cli_l6_v_beverage_sales_data_categorizados') }}
    GROUP BY customer_id, product
),

-- Productos del cluster con score de popularidad
productos_cluster AS (
    SELECT
        c.customer_id,
        TRIM(p.value::STRING) AS product,
        0.3 AS cluster_score
    FROM {{ ref('dwh_cli_t_fact_beverage_sales_data_categorizados') }} c
    JOIN {{ ref('dwh_cli_t_fact_beverage_sales_data_summary') }} s
      ON c.cluster = s.cluster,
    LATERAL FLATTEN(input => SPLIT(s.top_products_by_volume, ',')) p
),

-- Co-ocurrencias
emparejados AS (
    SELECT
        pc.customer_id,
        co.product_b AS product,
        SUM(co.cooccurrence_count) * 0.7 AS cooc_score
    FROM productos_cliente pc
    JOIN {{ ref('ods_cli_l7_v_beverage_sales_data_categorizados') }} co
      ON pc.product = co.product_a
    GROUP BY pc.customer_id, co.product_b
),

-- Combinar scores de cluster y coocurrencias
ranking_combinado_raw AS (
    SELECT
        customer_id,
        product,
        cooc_score AS total_score
    FROM emparejados

    UNION ALL

    SELECT
        customer_id,
        product,
        cluster_score AS total_score
    FROM productos_cluster
),

ranking_combinado AS (
    SELECT
        customer_id,
        product,
        SUM(total_score) AS total_score
    FROM ranking_combinado_raw
    GROUP BY customer_id, product
),

-- Productos con baja cantidad por cliente
productos_baja_cantidad AS (
    SELECT
        customer_id,
        product,
        total_quantity
    FROM productos_cliente
    WHERE total_quantity < 50
),

-- Filtramos para que solo salgan productos que el cliente compró poco pero que tienen buen score
productos_cross_sell AS (
    SELECT
        rc.customer_id,
        rc.product,
        rc.total_score,
        ROW_NUMBER() OVER (PARTITION BY rc.customer_id ORDER BY rc.total_score DESC) AS ranking
    FROM ranking_combinado rc
    JOIN productos_baja_cantidad pb
      ON rc.customer_id = pb.customer_id AND rc.product = pb.product
)

-- Resultado final
SELECT
    customer_id,
    product,
    total_score AS score,
    ranking,
    'cross_sell' AS recommendation_type
FROM productos_cross_sell
WHERE ranking <= 10
