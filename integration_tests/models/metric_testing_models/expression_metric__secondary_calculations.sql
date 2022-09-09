  select *
  from 
  {{ dbt_metrics.calculate(
      metric('ratio_metric'), 
      grain='day', 
      dimensions=['had_discount']
      )
  }}