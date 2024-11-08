With contact_name_per_order as (
    SELECT 
            distinct 
            order_ir,
            contact_full_name
    FROM 
        {{ref('int_clients_sch_contact_full_name')}}
)

Select *
from contact_name_per_order