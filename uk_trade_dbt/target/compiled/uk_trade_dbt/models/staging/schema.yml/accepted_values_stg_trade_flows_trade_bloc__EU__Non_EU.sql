
    
    

with all_values as (

    select
        trade_bloc as value_field,
        count(*) as n_records

    from "trade_db"."public"."stg_trade_flows"
    group by trade_bloc

)

select *
from all_values
where value_field not in (
    'EU','Non-EU'
)


