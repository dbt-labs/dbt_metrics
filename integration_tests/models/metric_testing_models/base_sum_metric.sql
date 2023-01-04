select *
from 
{{ metrics.calculate(
    [metric('base_sum_metric'), metric('base_sum_metric_duplicate')],
    secondary_calculations=[
        metrics.period_over_period(comparison_strategy="difference", interval=1, alias = "1mth")
    ]
    )
    }}