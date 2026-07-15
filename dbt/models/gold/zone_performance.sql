select
    pickup.borough as pickup_borough,
    pickup.zone_name as pickup_zone,
    count(*) as trip_count,
    round(sum(f.total_amount), 2) as total_revenue,
    round(avg(f.total_amount), 2) as average_trip_revenue,
    round(avg(f.trip_distance_miles)::numeric, 2) as average_distance_miles,
    round(avg(f.trip_duration_minutes)::numeric, 2) as average_duration_minutes
from {{ source('warehouse', 'fact_trips') }} f
join {{ source('warehouse', 'dim_location') }} pickup
  on pickup.location_key = f.pickup_location_key
group by pickup.borough, pickup.zone_name
