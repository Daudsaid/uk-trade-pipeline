
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_flow_id
from "trade_db"."public"."stg_trade_flows"
where trade_flow_id is null



  
  
      
    ) dbt_internal_test