select *
from {{ metrics.calculate(
            metric('case_when_metric'),
            grain='day'
        )
}}