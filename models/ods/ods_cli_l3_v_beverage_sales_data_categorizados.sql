SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS num_orders,  -- Frecuencia de compra: número de pedidos únicos por cliente
    SUM(total_spent) AS customer_value,  -- Valor total de vida del cliente
    AVG(total_quantity) AS avg_units_per_order,  -- Promedio de unidades por pedido
    SUM(total_spent) / NULLIF(COUNT(DISTINCT order_id), 0) AS avg_spent_per_order,  -- Promedio gastado por pedido
    AVG(days_since_last_order) AS avg_days_between_orders
FROM {{ ref('ods_cli_l2_v_beverage_sales_data_categorizados') }}
GROUP BY customer_id