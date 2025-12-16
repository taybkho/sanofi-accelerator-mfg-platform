with base as (
    select *
    from MFG_ACCELERATOR.ANALYTICS_analytics.stg_batches
),

deduped as (
    select *
    from base
    qualify row_number() over (
        partition by batch_id, load_run_id
        order by loaded_at desc
    ) = 1
)

select
    *,
    datediff('day', manufacture_date, expiry_date) as shelf_life_days,
    datediff('day', manufacture_date, current_date()) as batch_age_days,
    datediff('day', current_date(), expiry_date) as days_to_expiry,
    case when current_date() > expiry_date then 1 else 0 end as is_expired
from deduped