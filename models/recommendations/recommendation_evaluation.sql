WITH recommended AS (
    SELECT
        customer_id,
        product,
        ranking AS predicted_rank
    FROM {{ ref('customer_recommendations') }}
),

actuals AS (
    SELECT
        customer_id,
        product,
        total_quantity,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_quantity DESC) AS actual_rank
    FROM {{ ref('ods_cli_l6_v_beverage_sales_data_categorizados') }}
),

combined AS (
    SELECT
        r.customer_id,
        r.product,
        r.predicted_rank,
        a.total_quantity,
        a.actual_rank
    FROM recommended r
    LEFT JOIN actuals a
      ON r.customer_id = a.customer_id
     AND r.product = a.product
),

dcg_per_customer AS (
    SELECT
        customer_id,
        SUM(CASE 
            WHEN total_quantity IS NOT NULL THEN total_quantity / LOG(2, predicted_rank + 1)
            ELSE 0
        END) AS dcg
    FROM combined
    GROUP BY customer_id
),

ideal_dcg_per_customer AS (
    SELECT
        customer_id,
        SUM(total_quantity / LOG(2, actual_rank + 1)) AS ideal_dcg
    FROM actuals
    GROUP BY customer_id
)

SELECT
    dcg.customer_id,
    dcg.dcg,
    ideal.ideal_dcg,
    CASE 
        WHEN ideal.ideal_dcg = 0 THEN 0
        ELSE dcg.dcg / ideal.ideal_dcg
    END AS ndcg
FROM dcg_per_customer dcg
JOIN ideal_dcg_per_customer ideal
  ON dcg.customer_id = ideal.customer_id
