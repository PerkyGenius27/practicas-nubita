SELECT
  a.customer_id,
  a.product AS product_a,
  b.product AS product_b,
  COUNT(DISTINCT a.order_id) AS cooccurrence_count
FROM {{ ref('ods_syn_l1_v_beverage_sales_data_limpio') }} a
JOIN {{ ref('ods_syn_l1_v_beverage_sales_data_limpio') }} b
  ON a.order_id = b.order_id
  AND a.product < b.product
  AND a.customer_id = b.customer_id
GROUP BY a.customer_id, a.product, b.product
