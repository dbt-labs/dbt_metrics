select

  period,
  has_messaged,
  slack_joiners,
  ytd_sum,
  max_for_month,
  pop_1mth,
  ratio_to_1_month_ago,
  trunc(avg_3mth, 3) as avg_3mth, -- different databases return this ratio differently
  rolling_sum_3_month

from 
{{ metrics.metric(
    metric_name="slack_joiners", 
    grain='month', 
    dimensions=['has_messaged'], 
    start_date = '2021-01-01',
    end_date = '2021-04-01',
    secondary_calculations=[
      {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "difference", "alias": "pop_1mth"},
      {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "ratio"},
      {"calculation": "period_to_date", "aggregate": "sum", "period": "year", "alias": "ytd_sum"},
      {"calculation": "period_to_date", "aggregate": "max", "period": "month"},
      {"calculation": "rolling", "interval": 3, "aggregate": "average", "alias": "avg_3mth"},
      {"calculation": "rolling", "interval": 3, "aggregate": "sum"},
    ]) 
}}