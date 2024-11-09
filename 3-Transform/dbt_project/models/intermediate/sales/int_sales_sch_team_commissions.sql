WITH invoices AS (
    SELECT DISTINCT 
        *
    FROM 
        {{ref('int_sales_sch_invoices')}}
),

sales_owners AS (
    SELECT distinct *
    FROM 
        {{ref('stg_finance_erp_sch_sales_owners')}}
    where salesowners_order <= 3 
),
sales_owners_commission AS (
        Select 
                order_id,
                sales_owners,
                case 
                    when salesowners_order = 1 then 0.06
                    when salesowners_order = 2 then 0.025
                    when salesowners_order = 3 then 0.0095
                end as commissions_percentage
        from sales_owners
),
earnigns as (
        select 
            a.order_id,
            b.sales_owners,
            a.net_value * b.commissions_percentage as earnings
        From invoices as a
        inner join  sales_owners_commission as b 
        on a.order_id = b.order_id
)
select 
    sales_owners,
    ROUND(CAST(sum(earnings) AS NUMERIC), 2) as total_earnings
from earnigns
group by  sales_owners
order by total_earnings desc