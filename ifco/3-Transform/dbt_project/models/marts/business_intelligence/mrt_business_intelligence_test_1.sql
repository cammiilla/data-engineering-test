With orders_data as (
    SELECT order_id,
            company_name,
            crate_type
    FROM 
        {{ref('int_sales_sch_orders')}}
)
SELECT 
    company_name,
    COUNT(CASE WHEN crate_type = 'Wood' THEN 1 END) AS wood_orders,
    COUNT(CASE WHEN crate_type = 'Metal' THEN 1 END) AS metal_orders,
    COUNT(CASE WHEN crate_type = 'Plastic' THEN 1 END) AS plastic_orders
FROM orders_data
GROUP BY company_name
ORDER BY company_name