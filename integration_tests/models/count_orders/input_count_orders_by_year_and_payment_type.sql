select * from {{
  metrics.metric(
    'count_orders',
    'year',
    dimensions=['payment_type']
  )
}}