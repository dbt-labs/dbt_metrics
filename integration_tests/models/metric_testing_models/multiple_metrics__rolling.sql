select *
from 
{{ metrics.metric(
    [metric('sum_order_total'), metric('sum_discount_total')], 
    grain='day', 
    dimensions=['had_discount'], 
    secondary_calculations=[
      {"calculation": "rolling", "interval": 3, "aggregate": "average", "alias": "avg_3mth"},
      {"calculation": "rolling", "interval": 3, "aggregate": "sum"}
    ] 
    )
}}