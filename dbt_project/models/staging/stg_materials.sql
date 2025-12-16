select
    material_id,
    material_name,
    material_type,
    unit_of_measure,
    status,
    cast(created_at as date) as created_at,
    load_run_id,
    loaded_at
from {{ source('raw', 'materials') }}

