{% set my_metric_yml -%}

metrics:
  - name: develop_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: sum
    expression: discount_total
    window: 
        count: 14 
        period: days
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ dbt_metrics.develop(
        develop_yml=my_metric_yml,
        grain='week'
        )
    }}