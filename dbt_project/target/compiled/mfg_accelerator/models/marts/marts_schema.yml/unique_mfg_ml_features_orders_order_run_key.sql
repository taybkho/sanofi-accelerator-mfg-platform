
    
    

select
    order_run_key as unique_field,
    count(*) as n_records

from MFG_ACCELERATOR.ANALYTICS_analytics.mfg_ml_features_orders
where order_run_key is not null
group by order_run_key
having count(*) > 1


