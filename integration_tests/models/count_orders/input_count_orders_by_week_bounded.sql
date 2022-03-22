-- depends on: {{ ref('mock_purchase_data') }}

select * from {{
  metrics.metric(
    'count_orders',
    'week',
    start_date='2021-02-17',
    end_date='2023-01-01'
  )
}}