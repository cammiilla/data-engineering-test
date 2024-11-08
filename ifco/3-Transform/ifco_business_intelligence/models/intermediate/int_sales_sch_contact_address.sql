with orders as (
    select 
        *
    from 
        {{source('finance_erp', 'orders') }}
)