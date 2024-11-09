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
        COALESCE(NULLIF(contact_name, ''), 'John') as contact_name,
        COALESCE(NULLIF(contact_surname, ''), 'Doe') as contact_surname   
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
    