
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select order_run_key
from MFG_ACCELERATOR.ANALYTICS_analytics.mfg_ml_features_orders
where order_run_key is null



  
  
      
    ) dbt_internal_test