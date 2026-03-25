
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period
from "trade_db"."public"."stg_trade_flows"
where period is null



  
  
      
    ) dbt_internal_test