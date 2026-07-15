select
    d.year,
    d.month,
    min(d.month_name) as month_name,
    count(*) as trip_count,
    round(sum(f.fare_amount), 2) as fare_revenue,
    round(sum(f.tip_amount), 2) as tip_revenue,
    round(sum(f.tolls_amount), 2) as toll_revenue,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.total_amount), 2) as average_trip_revenue
from {{ source('warehouse', 'fact_trips') }} f
join {{ source('warehouse', 'dim_date') }} d
  on d.date_key = f.pickup_date_key
group by d.year, d.month
