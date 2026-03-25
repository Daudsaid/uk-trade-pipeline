
  
    

  create  table "trade_db"."public"."mart_top_imports__dbt_tmp"
  
  
    as
  
  (
    with by_commodity as (
    select * from "trade_db"."public"."int_trade_by_commodity"
)

select
    commodity_id,
    commodity_desc,
    trade_bloc,
    total_value_gbp,
    total_net_mass_kg,
    transaction_count
from by_commodity
where flow_direction = 'import'
order by total_value_gbp desc
limit 20
  );
  