select * from {{
  metrics.metric(
    'count_orders',
    'year'
  )
}}