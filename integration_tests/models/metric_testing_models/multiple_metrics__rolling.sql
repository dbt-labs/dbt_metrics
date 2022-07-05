select *
from 
{{ metrics.metric(
    [metric('base_sum_metric'), metric('base_average_metric')], 
    grain='day', 
    dimensions=['had_discount'], 
    secondary_calculations=[
      {"calculation": "rolling", "interval": 3, "aggregate": "average", "alias": "avg_3mth"},
      {"calculation": "rolling", "interval": 3, "aggregate": "sum"}
    ] 
    )
}}