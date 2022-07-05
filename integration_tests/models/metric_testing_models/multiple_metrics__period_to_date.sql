select *
from 
{{ metrics.metric(
    [metric('sum_order_total'), metric('sum_discount_total')], 
    grain='day', 
    dimensions=['had_discount'], 
    secondary_calculations=[
      {"calculation": "period_to_date", "aggregate": "sum", "period": "year", "alias": "ytd_sum"},
      {"calculation": "period_to_date", "aggregate": "max", "period": "month"},
    ] 
    )
}}

