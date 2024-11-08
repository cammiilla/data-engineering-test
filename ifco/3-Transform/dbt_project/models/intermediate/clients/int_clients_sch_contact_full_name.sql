with orders as (
    select 
        order_id,
        contact_name,
        contact_surname
    from 
        {{ref('stg_finance_erp_sch_orders')}}
),
difine_na_rules as (
    select 
        order_id,
        case 
            when contact_name = 'N/A' then 'John'
            else contact_name 
        end as contact_name,
        case 
            when contact_surname = 'N/A' then 'Doe'
            else contact_surname 
        end as contact_surname   
    from orders
),
contact_full_name as (
    select 
        order_id,
        contact_name || ' ' || contact_surname AS contact_full_name
    from difine_na_rules
)

Select 
    *
from 
    contact_full_name
    