select *
from 
{{ metrics.calculate(
    metric('base_count_metric'), 
    grain='month', 
    start_date = '2021-01-01',
    end_date = '2021-04-01',
    secondary_calculations=[
      {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "difference", "alias": "pop_1mth"},
      {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "ratio"},
      {"calculation": "period_to_date", "aggregate": "sum", "period": "year", "alias": "ytd_sum"},
      {"calculation": "period_to_date", "aggregate": "max", "period": "month"},
      {"calculation": "rolling", "interval": 3, "aggregate": "average", "alias": "avg_3mth"},
      {"calculation": "rolling", "interval": 3, "aggregate": "sum"},
    ] 
    )
}}