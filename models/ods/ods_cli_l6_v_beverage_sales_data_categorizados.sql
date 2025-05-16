SELECT
    customer_id,
    product,
    category,
    region,
    
    COUNT(DISTINCT order_id) AS num_orders,
    SUM(quantity) AS total_quantity,
    SUM(total_price) AS total_spent,
    AVG(unit_price) AS avg_unit_price,
    AVG(discount) AS avg_discount,
    MIN(order_date) AS first_order_date,
    MAX(order_date) AS last_order_date,
    COUNT(*) AS num_lines

FROM {{ ref('ods_syn_l1_v_beverage_sales_data_limpio') }}
GROUP BY
    customer_id, product, category, region
