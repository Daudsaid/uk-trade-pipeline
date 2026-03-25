with by_commodity as (
    select * from {{ ref('int_trade_by_commodity') }}
)

select
    commodity_id,
    commodity_desc,
    trade_bloc,
    total_value_gbp,
    total_net_mass_kg,
    transaction_count
from by_commodity
where flow_direction = 'export'
order by total_value_gbp desc
limit 20
