select
    coalesce(payment.payment_type_name, 'Missing') as payment_type,
    count(*) as trip_count,
    round(sum(f.total_amount), 2) as total_revenue,
    round(sum(f.tip_amount), 2) as total_tips,
    round(avg(f.tip_amount), 2) as average_tip,
    round(100.0 * count(*) / sum(count(*)) over (), 2) as trip_percentage
from {{ source('warehouse', 'fact_trips') }} f
left join {{ source('warehouse', 'dim_payment_type') }} payment
  on payment.payment_type_key = f.payment_type_key
group by coalesce(payment.payment_type_name, 'Missing')
