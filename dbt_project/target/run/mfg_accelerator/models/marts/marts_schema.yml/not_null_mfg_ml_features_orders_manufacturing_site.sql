
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select manufacturing_site
from MFG_ACCELERATOR.ANALYTICS_analytics.mfg_ml_features_orders
where manufacturing_site is null



  
  
      
    ) dbt_internal_test