

with batches_by_day as (

    select
        manufacturing_site,
        manufacture_date as calendar_date,
        load_run_id,

        count(*) as total_batches,
        sum(quantity) as total_quantity,
        sum(case when is_expired = 1 then 1 else 0 end) as expired_batches

    from MFG_ACCELERATOR.ANALYTICS_analytics.core_batches
    group by manufacturing_site, manufacture_date, load_run_id

),

orders_by_day as (

    select
        b.manufacturing_site,
        o.planned_start_date as calendar_date,
        o.load_run_id,

        count(*) as total_orders,
        sum(case when o.actual_end_date is not null then 1 else 0 end) as completed_orders,
        sum(case when o.end_delay_days > 0 then 1 else 0 end) as late_orders,
        avg(o.end_delay_days) as avg_delay_vs_plan_days

    from MFG_ACCELERATOR.ANALYTICS_analytics.fct_production_orders o
    join MFG_ACCELERATOR.ANALYTICS_analytics.core_batches b
        on o.batch_id = b.batch_id
       and o.load_run_id = b.load_run_id

    group by
        b.manufacturing_site,
        o.planned_start_date,
        o.load_run_id
)

select
    coalesce(b.manufacturing_site, o.manufacturing_site) as manufacturing_site,
    coalesce(b.calendar_date, o.calendar_date) as calendar_date,
    coalesce(b.load_run_id, o.load_run_id) as load_run_id,

    b.total_batches,
    b.total_quantity,
    b.expired_batches,

    o.total_orders,
    o.completed_orders,
    o.late_orders,
    o.avg_delay_vs_plan_days,

    case
        when o.total_orders > 0
        then 1 - (o.late_orders / o.total_orders)
        else null
    end as on_time_rate

from batches_by_day b
full outer join orders_by_day o
    on b.manufacturing_site = o.manufacturing_site
   and b.calendar_date = o.calendar_date
   and b.load_run_id = o.load_run_id