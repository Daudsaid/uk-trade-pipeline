
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select flow_type
from "trade_db"."public"."stg_trade_flows"
where flow_type is null



  
  
      
    ) dbt_internal_test