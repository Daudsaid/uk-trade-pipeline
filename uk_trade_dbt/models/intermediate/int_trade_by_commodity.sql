with stg as (
    select * from {{ ref('stg_trade_flows') }}
)

select
    commodity_id,
    commodity_desc,
    flow_direction,
    trade_bloc,
    sum(value_gbp)    as total_value_gbp,
    sum(net_mass_kg)  as total_net_mass_kg,
    count(*)          as transaction_count
from stg
group by
    commodity_id,
    commodity_desc,
    flow_direction,
    trade_bloc
