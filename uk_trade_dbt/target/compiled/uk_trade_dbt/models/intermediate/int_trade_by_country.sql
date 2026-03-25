with stg as (
    select * from "trade_db"."public"."stg_trade_flows"
)

select
    country_id,
    country_name,
    flow_direction,
    trade_bloc,
    sum(value_gbp)    as total_value_gbp,
    sum(net_mass_kg)  as total_net_mass_kg,
    count(*)          as transaction_count
from stg
group by
    country_id,
    country_name,
    flow_direction,
    trade_bloc