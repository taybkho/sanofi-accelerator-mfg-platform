with base as (
    select *
    from {{ ref('stg_production_orders') }}
),

deduped as (
    select *
    from base
    qualify row_number() over (
        partition by order_id, load_run_id
        order by loaded_at desc
    ) = 1
)

select
    *,
    datediff('day', planned_start_date, actual_start_date) as start_delay_days,
    datediff('day', planned_end_date, actual_end_date) as end_delay_days
from deduped
