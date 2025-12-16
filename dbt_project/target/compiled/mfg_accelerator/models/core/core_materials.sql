with base as (
    select *
    from MFG_ACCELERATOR.ANALYTICS_analytics.stg_materials
),

deduped as (
    select *
    from base
    qualify row_number() over (
        partition by material_id, load_run_id
        order by loaded_at desc
    ) = 1
)

select *
from deduped