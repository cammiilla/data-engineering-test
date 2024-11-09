With contact_adress_per_order as (
    SELECT 
            distinct 
            order_id,
            contact_address
    FROM 
        {{ref('int_clients_sch_contact_address')}}
)

Select *
from contact_adress_per_order