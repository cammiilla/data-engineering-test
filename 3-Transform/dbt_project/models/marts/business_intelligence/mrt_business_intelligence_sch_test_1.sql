With distribuition as (
    SELECT *
    FROM 
        {{ref('int_sales_sch_distribuition_crate_type_per_company')}}
)
SELECT 
        *
FROM distribuition
