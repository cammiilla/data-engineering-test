with sales_owners as (
    select 
        *
    from 
        {{source('bronze', 'sales_owners') }}
)
select distinct
    order_id,
    salesowner as sales_owners,
    salesowners_order
from sales_owners
