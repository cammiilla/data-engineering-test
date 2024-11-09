WITH invoices AS (
    SELECT DISTINCT 
        id,
        order_id,
        gross_value,
        vat
    FROM 
        {{ref('stg_finance_erp_sch_invoices')}}
),
net_value as (
    SELECT DISTINCT 
        id,
        order_id,
        gross_value,
        vat,
        gross_value-vat as net_value
    FROM 
        invoices
),
value_in_euro as (
    SELECT DISTINCT 
        id,
        order_id,
        gross_value / 100 as gross_nuevo,
        vat / 100 as vat,
        net_value / 100 as net_value
    FROM 
        net_value
)
Select 
        *
from value_in_euro