with team_commissions as (
        select 
            sales_owners,
            total_earnings
        from {{ref('int_sales_sch_team_commissions')}}
)
Select *
from team_commissions