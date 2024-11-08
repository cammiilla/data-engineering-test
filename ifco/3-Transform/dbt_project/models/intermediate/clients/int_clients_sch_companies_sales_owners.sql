WITH orders AS (
    SELECT distinct *
    FROM 
        {{ref('int_sales_sch_orders')}}
),
sales_owners AS (
    SELECT distinct *
    FROM 
        {{ref('stg_finance_erp_sch_sales_owners')}}
),
join_orders_sales_owners as (
    select 
    a.order_id    
    , date
    , company_id
    , company_name
    , crate_type
    , contact_name
    , contact_surname
    , city
    , cp
    , sales_owners
    from orders as a 
    inner join sales_owners as b
    on a.order_id = b.order_id
),
unique_register_by_order_id as (
    select distinct 
        order_id,
        company_name,
        sales_owners
    from join_orders_sales_owners
)
SELECT 
    company_name,
    STRING_AGG(DISTINCT sales_owners, ', ' ORDER BY sales_owners) AS sales_owners_list
FROM unique_register_by_order_id
GROUP BY company_name
ORDER BY company_name