-- depends on: {{ ref('mock_purchase_data') }}

select * from {{
  metrics.metric(
    metric_name='sum_order_revenue',
    grain='month',
    dimensions=['payment_type']
  )
}}