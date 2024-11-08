with invoices as (
    select 
        *
    from 
        {{source('finance_erp', 'invoices') }}
)
select 
    id
    , orderId as order_id
    , companyId as company_id
    , grossValue as gross_value
    , vat as vat
from 
    invoices
