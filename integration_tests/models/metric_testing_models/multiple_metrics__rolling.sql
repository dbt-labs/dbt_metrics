select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_average_metric')], 
    grain='day', 
    dimensions=['had_discount'], 
    secondary_calculations=[
      {"calculation": "rolling", "interval": 3, "aggregate": "min", "alias": "min_3mth"},
      {"calculation": "rolling", "interval": 3, "aggregate": "max", "alias": "max_3mth"}
    ] 
    )
}}