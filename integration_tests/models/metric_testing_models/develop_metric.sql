{% set my_metric_yml -%}

metrics:
  - name: develop_metric
    model: ref('fact_orders')
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: average
    expression: discount_total
    dimensions:
      - had_discount
      - order_country

{%- endset %}

select * 
from {{ dbt_metrics.develop(
        develop_yml=my_metric_yml,
        grain='month'
        )
    }}