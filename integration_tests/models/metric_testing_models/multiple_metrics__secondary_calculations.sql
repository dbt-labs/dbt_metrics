select *
from 
{{ metrics.metric(
    [metric('sum_order_total'), metric('sum_discount_total')], 
    grain='day', 
    dimensions=['had_discount'], 
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