
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_balance_gbp
from "trade_db"."public"."mart_trade_summary"
where trade_balance_gbp is null



  
  
      
    ) dbt_internal_test