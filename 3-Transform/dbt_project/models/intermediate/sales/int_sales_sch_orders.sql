WITH orders_company_name_selection AS (
    SELECT DISTINCT 
        order_id,
        company_id,
        company_name
    FROM 
        {{ref('stg_finance_erp_sch_orders')}}
),
orders AS (
    SELECT *
    FROM 
        {{ref('stg_finance_erp_sch_orders')}}
),

company_name_parts AS (
    SELECT 
        company_id,
        company_name, 
        REPLACE(SPLIT_PART(company_name, ' ', 1), '.', '') AS part1,
        REPLACE(SPLIT_PART(company_name, ' ', 2), '.', '') AS part2,
        REPLACE(SPLIT_PART(company_name, ' ', 3), '.', '') AS part3
    FROM orders_company_name_selection
),

company_name_concatenated AS (
    SELECT 
        company_id,
        company_name,
        part1 || ' ' || part2 || ' ' || part3 AS company_name_new
    FROM company_name_parts
), 

company_name_cleaned AS (
    SELECT 
        company_id,
        company_name,
        TRIM(REGEXP_REPLACE(company_name_new, '\y(inc|ltd|co|unlimited)\y', '', 'gi')) AS company_name_new
    FROM company_name_concatenated
),

company_new_id AS (
    SELECT 
        company_name_new,
        min(company_id) AS company_id_new
    FROM company_name_cleaned
    GROUP BY company_name_new
    ORDER BY company_name_new ASC 
),

newer_company_definition as (
    select 
        a.company_id,
        a.company_name,
        b.company_name_new,
        b.company_id_new
    from company_name_cleaned as a
    inner join company_new_id as b
    on a.company_name_new = b.company_name_new

)

-- This final SELECT will show the processed company names and their count
SELECT 
    a.order_id
    , a.date
    , b.company_id_new as company_id
    , b.company_name_new as company_name
    , a.crate_type
    , a.contact_name
    , a.contact_surname
    , a.city
    , a.cp
FROM orders a
inner join newer_company_definition b
on a.company_id = b.company_id
and a.company_name = b.company_name

