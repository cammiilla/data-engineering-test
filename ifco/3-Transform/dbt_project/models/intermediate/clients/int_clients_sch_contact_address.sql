with orders as (
    select 
        *
    from 
        {{ref('stg_finance_erp_sch_orders')}}
),
difine_unknown_rules as (
    select 
        order_id,
        case 
            when city = null then 'Unknown'
            else city 
        end as city_name,
        case 
            when cp = null then 'UNK00'
            else cp 
        end as postal_code   
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