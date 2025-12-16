
    
    

select
    __grain_check__ as unique_field,
    count(*) as n_records

from MFG_ACCELERATOR.ANALYTICS_analytics.core_materials
where __grain_check__ is not null
group by __grain_check__
having count(*) > 1


