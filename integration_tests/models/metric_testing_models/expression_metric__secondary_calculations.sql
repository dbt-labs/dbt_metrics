  select *
  from 
  {{ metrics.calculate(
      metric('ratio_metric'), 
      grain='day', 
      dimensions=['had_discount'], 
      secondary_calculations=[
        metrics.rolling(aggregate="average", interval=7)
      ] 
      )
  }}