with sales_owners as (
    select 
        *
    from 
        {{source('finance_erp', 'sales_owners') }}
)
select distinct
    order_id,
    salesowners as sales_owners
from sales_owners
