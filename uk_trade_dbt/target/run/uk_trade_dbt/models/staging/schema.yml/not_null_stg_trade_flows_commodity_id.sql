
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select commodity_id
from "trade_db"."public"."stg_trade_flows"
where commodity_id is null



  
  
      
    ) dbt_internal_test