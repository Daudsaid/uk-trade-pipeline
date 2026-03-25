
  create view "trade_db"."public"."stg_trade_flows__dbt_tmp"
    
    
  as (
    with trade_flows as (
    select * from "trade_db"."public"."trade_flows"
),
dim_countries as (
    select * from "trade_db"."public"."dim_countries"
),
dim_commodities as (
    select * from "trade_db"."public"."dim_commodities"
),
dim_ports as (
    select * from "trade_db"."public"."dim_ports"
)
select
    tf.id           as trade_flow_id,
    tf.period,
    tf.flow_type,
    tf.value_gbp,
    tf.net_mass_kg,
    tf.loaded_at,
    tf.country_id,
    dc.country_name,
    dc.country_code,
    tf.commodity_id,
    dcom.commodity_code,
    dcom.commodity_desc,
    tf.port_id,
    dp.port_name,
    case
        when lower(tf.flow_type) like '%import%' then 'import'
        when lower(tf.flow_type) like '%export%' then 'export'
        else 'unknown'
    end as flow_direction,
    case
        when lower(tf.flow_type) like 'eu%' then 'EU'
        else 'Non-EU'
    end as trade_bloc
from trade_flows tf
left join dim_countries   dc   on tf.country_id   = dc.country_id
left join dim_commodities dcom on tf.commodity_id = dcom.commodity_id
left join dim_ports       dp   on tf.port_id      = dp.port_id
  );