with orders as (
    select 
        *
    from 
        {{ref('stg_finance_erp_sch_orders')}}
),
difine_unknown_rules as (
    select 
        order_id,
        COALESCE(NULLIF(city, ''), 'Unknown') as city_name,
        COALESCE(NULLIF(cp, ''), 'UNK00') as postal_code   
    from orders
),
contac_contact_address as (
    select 
        order_id,
        city_name || ', ' || postal_code AS contact_address
    from difine_unknown_rules
)

Select 
    *
from 
    contac_contact_address