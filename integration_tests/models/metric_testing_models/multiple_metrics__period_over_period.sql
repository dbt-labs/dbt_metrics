  select *
  from 
  {{ metrics.calculate(
      [metric('base_sum_metric'), metric('base_average_metric')], 
      grain='day', 
      dimensions=['had_discount'], 
      secondary_calculations=[
        metrics.period_over_period(
            comparison_strategy="difference"
            ,interval=1
            ,metric_list='base_sum_metric'
            )
          ] 
      )
  }}