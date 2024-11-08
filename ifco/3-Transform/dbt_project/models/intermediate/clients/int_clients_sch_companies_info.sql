with orders as (
    select distinct  
        company_id,
        company_name
    from 
        {{ref('stg_finance_erp_sch_orders')}}
),
count_company_id as (
    select 
        company_name,
        count(company_id) as count_id
    from orders
    group by company_name
)
select 
    *
from count_company_id
order by company_name asc
