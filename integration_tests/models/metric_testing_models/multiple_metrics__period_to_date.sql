select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_average_metric')], 
    grain='day', 
    dimensions=['had_discount'], 
    secondary_calculations=[
      {"calculation": "period_to_date", "aggregate": "min", "period": "year", "alias": "ytd_min"},
      {"calculation": "period_to_date", "aggregate": "max", "period": "month"},
    ] 
    )
}}

