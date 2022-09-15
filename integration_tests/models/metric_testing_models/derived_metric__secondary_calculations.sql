  select *
  from 
  {{ metrics.calculate(
      metric('ratio_metric'), 
      grain='day', 
      dimensions=['had_discount']
      )
  }}