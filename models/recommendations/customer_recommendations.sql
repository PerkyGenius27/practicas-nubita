WITH productos_cliente AS (
    SELECT
        customer_id,
        product
    FROM {{ ref('ods_cli_l6_v_beverage_sales_data_categorizados') }}
    GROUP BY customer_id, product
),

emparejados AS (
    SELECT
        pc.customer_id,
        co.product_b AS product,
        SUM(co.cooccurrence_count) * 0.4 AS score
    FROM productos_cliente pc
    JOIN {{ ref('ods_cli_l7_v_beverage_sales_data_categorizados') }} co
      ON pc.product = co.product_a
    GROUP BY pc.customer_id, co.product_b
),

productos_cluster AS (
    SELECT
        c.customer_id,
        TRIM(p.value::STRING) AS product,
        0.3 AS score
    FROM {{ ref('dwh_cli_t_fact_beverage_sales_data_categorizados') }} c
    JOIN {{ ref('dwh_cli_t_fact_beverage_sales_data_summary') }} s
      ON c.cluster = s.cluster
    , LATERAL FLATTEN(input => SPLIT(s.top_products_by_volume, ',')) p
),

ranking_combinado AS (
    SELECT
        customer_id,
        product,
        SUM(score) AS score
    FROM (
        SELECT * FROM emparejados
        UNION ALL
        SELECT * FROM productos_cluster
    )
    GROUP BY customer_id, product
),

productos_ya_comprados AS (
    SELECT
        customer_id,
        product
    FROM {{ ref('ods_cli_l6_v_beverage_sales_data_categorizados') }}
    GROUP BY customer_id, product
)

SELECT
    rc.customer_id,
    rc.product,
    rc.score * CASE WHEN pj.product IS NOT NULL THEN 0.5 ELSE 1 END AS score,
    ROW_NUMBER() OVER (PARTITION BY rc.customer_id ORDER BY rc.score DESC) AS ranking
FROM ranking_combinado rc
LEFT JOIN productos_ya_comprados pj
  ON rc.customer_id = pj.customer_id AND rc.product = pj.product
QUALIFY ranking <= 10
