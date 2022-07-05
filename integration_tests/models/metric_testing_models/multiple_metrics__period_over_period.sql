with metric as (
  select *
  from 
  {{ metrics.metric(
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
  "DATE_DAY"
  ,"HAD_DISCOUNT"
  ,"BASE_SUM_METRIC"
  ,"BASE_AVERAGE_METRIC"
  ,"BASE_SUM_METRIC_POP_1MTH"
  ,"BASE_SUM_METRIC_RATIO_TO_1_DAY_AGO"
  ,"BASE_AVERAGE_METRIC_POP_1MTH"
  ,"BASE_AVERAGE_METRIC_RATIO_TO_1_DAY_AGO"::number(38,10) as "BASE_AVERAGE_METRIC_RATIO_TO_1_DAY_AGO" 
from metric 