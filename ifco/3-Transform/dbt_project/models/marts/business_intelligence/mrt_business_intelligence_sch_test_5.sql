with companies_sales_owners as (
        select 
            company_id,
            company_name,
            list_salesowners
        from {{ref('int_clients_sch_companies_sales_owners')}}
)
Select *
from companies_sales_owners