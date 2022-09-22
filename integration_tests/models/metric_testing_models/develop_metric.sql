{% set my_metric_yml -%}
{% raw %}

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

  - name: derived_metric
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{ metric('develop_metric') }} - 1 "
    dimensions:
      - had_discount
      - order_country

  - name: some_other_metric_not_using
    label: Total Discount ($)
    timestamp: order_date
    time_grains: [day, week, month]
    calculation_method: derived
    expression: "{{ metric('derived_metric') }} - 1 "
    dimensions:
      - had_discount
      - order_country

{% endraw %}
{%- endset %}

select * 
from {{ metrics.develop(
        develop_yml=my_metric_yml,
        metric_list=['derived_metric'],
        grain='month'
        )
    }} 