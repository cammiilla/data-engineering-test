with orders as (
    select 
        *
    from 
        {{ref('stg_finance_erp_sch_orders')}}
)
select 
    