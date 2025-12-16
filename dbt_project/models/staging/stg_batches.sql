select
    batch_id,
    material_id,
    manufacturing_site,
    cast(manufacture_date as date) as manufacture_date,
    cast(expiry_date as date) as expiry_date,
    cast(quantity as number) as quantity,
    batch_status,
    load_run_id,
    loaded_at
from {{ source('raw', 'batches') }}
