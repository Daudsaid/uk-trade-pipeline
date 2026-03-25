
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        flow_direction as value_field,
        count(*) as n_records

    from "trade_db"."public"."stg_trade_flows"
    group by flow_direction

)

select *
from all_values
where value_field not in (
    'import','export'
)



  
  
      
    ) dbt_internal_test