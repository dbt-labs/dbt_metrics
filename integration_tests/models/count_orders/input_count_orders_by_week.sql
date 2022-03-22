-- depends on: {{ ref('mock_purchase_data') }}

select * from {{
  metrics.metric(
    'count_orders',
    'week'
  )
}}