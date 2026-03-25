
  
    

  create  table "trade_db"."public"."int_trade_by_commodity__dbt_tmp"
  
  
    as
  
  (
    with stg as (
    select * from "trade_db"."public"."stg_trade_flows"
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
  );
  