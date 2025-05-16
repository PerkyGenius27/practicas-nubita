SELECT
    TRIM(Order_ID) AS order_id,
    TRIM(Customer_ID) AS customer_id,
    INITCAP(TRIM(Customer_Type)) AS customer_type,
    INITCAP(TRIM(Product)) AS product,
    INITCAP(TRIM(Category)) AS category,
    CAST(Unit_Price AS FLOAT) AS unit_price,
    CAST(Quantity AS INT) AS quantity,
    CAST(Discount AS FLOAT) AS discount,
    CAST(Total_Price AS FLOAT) AS total_price,
    INITCAP(TRIM(Region)) AS region,
    TO_DATE(Order_Date) AS order_date
FROM {{ source('lnd', 'lnd_syn_t_beverage_sales_data') }}
WHERE
    Order_ID IS NOT NULL
    AND Customer_ID IS NOT NULL
    AND Product IS NOT NULL