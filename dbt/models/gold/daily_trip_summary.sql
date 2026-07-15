select
    d.full_date as trip_date,
    d.day_name,
    d.is_weekend,
    count(*) as trip_count,
    sum(f.passenger_count) as passenger_count,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.total_amount), 2) as average_trip_revenue,
    round(avg(f.trip_distance_miles)::numeric, 2) as average_distance_miles,
    round(avg(f.trip_duration_minutes)::numeric, 2) as average_duration_minutes,
    round(sum(f.tip_amount), 2) as total_tips
from {{ source('warehouse', 'fact_trips') }} f
join {{ source('warehouse', 'dim_date') }} d
  on d.date_key = f.pickup_date_key
group by d.full_date, d.day_name, d.is_weekend
