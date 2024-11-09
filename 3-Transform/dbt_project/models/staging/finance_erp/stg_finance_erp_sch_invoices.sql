with invoices as (
    select 
        *
    from 
        {{source('bronze', 'invoices') }}
)
select 
    id
    , orderId as order_id
    , CAST(grossValue AS FLOAT) as gross_value
    , CAST(vat AS FLOAT)  as vat
from 
    invoices
