with metric as (
  select *
  from 
  {{ metrics.calculate(
      [metric('base_sum_metric'), metric('base_average_metric')], 
      grain='day', 
      dimensions=['had_discount'], 
      secondary_calculations=[
        {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "difference", "alias": "pop_1mth"},
        {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "ratio"},
      ] 
      )
  }}
)

select 
  date_day
  ,had_discount
  ,base_sum_metric
  ,cast(base_average_metric as float) as base_average_metric
  ,base_sum_metric_pop_1mth
  ,cast(base_sum_metric_ratio_to_1_day_ago as float) as base_sum_metric_ratio_to_1_day_ago
  ,cast(base_average_metric_pop_1mth as float) as base_average_metric_pop_1mth
  ,cast(base_average_metric_ratio_to_1_day_ago as float) as base_average_metric_ratio_to_1_day_ago
from metric 