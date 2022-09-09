  select *
  from 
  {{ dbt_metrics.calculate(
      [metric('base_sum_metric'), metric('base_average_metric')], 
      grain='day', 
      dimensions=['had_discount'], 
      secondary_calculations=[
        {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "difference", "alias": "pop_1mth"},
        {"calculation": "period_over_period", "interval": 1, "comparison_strategy": "ratio"},
      ] 
      )
  }}