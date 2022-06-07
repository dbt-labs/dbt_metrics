-- depends on: {{ ref('mock_purchase_data') }}

select * from {{
  metrics.metric(
    metric_name = 'count_orders',
    grain = 'day',
    where = ["purchased_at >= '2022-02-13'","payment_type = 'jcb'"]
  )
}}