select
    order_id,
    batch_id,
    order_type,
    cast(planned_start_date as date) as planned_start_date,
    cast(planned_end_date as date) as planned_end_date,
    cast(actual_start_date as date) as actual_start_date,
    cast(actual_end_date as date) as actual_end_date,
    order_status,
    line_id,
    load_run_id,
    loaded_at
from {{ source('raw', 'production_orders') }}
