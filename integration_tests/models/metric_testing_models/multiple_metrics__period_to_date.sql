select *
from 
{{ metrics.metric(
    [metric('base_sum_metric'), metric('base_average_metric')], 
    grain='day', 
    dimensions=['had_discount'], 
    secondary_calculations=[
      {"calculation": "period_to_date", "aggregate": "sum", "period": "year", "alias": "ytd_sum"},
      {"calculation": "period_to_date", "aggregate": "max", "period": "month"},
    ] 
    )
}}

