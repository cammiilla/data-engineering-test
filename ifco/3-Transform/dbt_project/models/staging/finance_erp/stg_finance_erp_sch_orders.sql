with orders as (
    select 
        *
    from 
        {{source('finance_erp', 'orders') }}
)
select 
    order_id
    , date
    , company_id
    , LOWER(company_name) as company_name
    , crate_type
    , contact_name
    , contact_surname
    , city
    , cp
from 
    orders
