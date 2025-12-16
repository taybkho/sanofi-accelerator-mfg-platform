
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select load_run_id
from MFG_ACCELERATOR.ANALYTICS_analytics.core_materials
where load_run_id is null



  
  
      
    ) dbt_internal_test