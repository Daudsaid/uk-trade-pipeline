
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_imports_gbp
from "trade_db"."public"."mart_trade_summary"
where total_imports_gbp is null



  
  
      
    ) dbt_internal_test