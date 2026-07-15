with gold_total as (
    select sum(trip_count) as row_count
    from {{ ref('daily_trip_summary') }}
),
fact_total as (
    select count(*) as row_count
    from {{ source('warehouse', 'fact_trips') }}
)
select gold_total.row_count
from gold_total
cross join fact_total
where gold_total.row_count <> fact_total.row_count
