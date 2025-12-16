{{ config(materialized='table') }}

with orders as (

    select
        order_id,
        batch_id,
        order_type,
        order_status,
        line_id,
        planned_start_date,
        planned_end_date,
        actual_start_date,
        actual_end_date,
        start_delay_days,
        end_delay_days,
        load_run_id
    from {{ ref('fct_production_orders') }}

),

batches as (

    select
        batch_id,
        material_id,
        manufacturing_site,
        batch_status,
        quantity,
        shelf_life_days,
        batch_age_days,
        days_to_expiry,
        is_expired,
        load_run_id
    from {{ ref('core_batches') }}

),

materials as (

    select
        material_id,
        material_type,
        unit_of_measure,
        case when status = 'ACTIVE' then 1 else 0 end as is_active_material,
        load_run_id
    from {{ ref('core_materials') }}

)

select
    o.order_id,

    b.manufacturing_site,
    o.line_id,
    o.order_type,
    o.order_status,

    m.material_type,
    m.unit_of_measure,
    b.batch_status,
    m.is_active_material,

    b.quantity,
    b.shelf_life_days,
    b.batch_age_days,
    b.days_to_expiry,
    b.is_expired,

    o.planned_start_date,
    o.planned_end_date,
    o.actual_start_date,
    o.actual_end_date,

    datediff('day', o.planned_start_date, o.planned_end_date) as planned_duration_days,
    datediff('day', o.actual_start_date, o.actual_end_date) as actual_duration_days,

    o.end_delay_days as delay_vs_plan_days,
    case when o.end_delay_days > 0 then 1 else 0 end as is_late,
    case when o.actual_end_date is not null then 1 else 0 end as is_completed

from orders o
left join batches b
    on o.batch_id = b.batch_id
   and o.load_run_id = b.load_run_id
left join materials m
    on b.material_id = m.material_id
   and b.load_run_id = m.load_run_id
