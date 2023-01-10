select *
from {{ metrics.calculate(metric('base_sum_metric'))}}