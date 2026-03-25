
  
    

  create  table "trade_db"."public"."mart_trade_summary__dbt_tmp"
  
  
    as
  
  (
    with by_country as (
    select * from "trade_db"."public"."int_trade_by_country"
),

aggregated as (
    select
        trade_bloc,
        sum(case when flow_direction = 'import' then total_value_gbp else 0 end) as total_imports_gbp,
        sum(case when flow_direction = 'export' then total_value_gbp else 0 end) as total_exports_gbp,
        sum(total_value_gbp) as total_trade_gbp
    from by_country
    group by trade_bloc
)

select
    trade_bloc,
    total_imports_gbp,
    total_exports_gbp,
    total_trade_gbp,
    total_exports_gbp - total_imports_gbp as trade_balance_gbp
from aggregated
order by trade_bloc
  );
  