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
    secondary_calcs=[
        {"type": "period_to_date", "aggregate": "sum", "period": "year", "alias": "ytd_sum"},
        {"type": "period_to_date", "aggregate": "max", "period": "month"},
        {"type": "period_over_period", "lag": 1, "how": "difference", "alias": "pop_1mth"},
        {"type": "period_over_period", "lag": 1, "how": "ratio"},
        {"type": "rolling", "window": 3, "aggregate": "average", "alias": "avg_3mth"},
        {"type": "rolling", "window": 3, "aggregate": "sum"},
    ]) 
}}

where period >= '2021-01-01' and period < '2021-05-01'