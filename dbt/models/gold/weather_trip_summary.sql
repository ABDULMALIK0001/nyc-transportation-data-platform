select
    case
        when w.precipitation_mm > 0 then 'Rain'
        when w.temperature_celsius < 10 then 'Cold and dry'
        when w.temperature_celsius >= 25 then 'Warm and dry'
        else 'Mild and dry'
    end as weather_condition,
    count(*) as trip_count,
    round(avg(w.temperature_celsius)::numeric, 2) as average_temperature_celsius,
    round(avg(f.trip_duration_minutes)::numeric, 2) as average_duration_minutes,
    round(avg(f.trip_distance_miles)::numeric, 2) as average_distance_miles,
    round(sum(f.total_amount), 2) as total_revenue
from {{ source('warehouse', 'fact_trips') }} f
join {{ source('warehouse', 'dim_weather') }} w
  on w.weather_key = f.weather_key
group by 1
