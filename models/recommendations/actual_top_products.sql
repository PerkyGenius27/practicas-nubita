SELECT
  customer_id,
  product,
  total_quantity,
  ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_quantity DESC) AS actual_rank
FROM {{ ref('ods_cli_l6_v_beverage_sales_data_categorizados') }}