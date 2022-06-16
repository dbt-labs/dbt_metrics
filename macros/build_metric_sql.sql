{%- macro build_metric_sql(metric, model, grain, dimensions, secondary_calculations, start_date, end_date, where, calendar_tbl,relevant_periods) %}

    {# This is the SQL Gen part - we've broken each component out into individual macros #}
    {# We broke this out so it can loop for composite metrics #}
    {{metrics.gen_aggregate_cte(metric,model,grain,dimensions,secondary_calculations, start_date, end_date, where, calendar_tbl,relevant_periods)}}
    {{metrics.gen_dimensions_cte(metric,dimensions)}}
    {{metrics.gen_spine_time_cte(metric,grain,dimensions,secondary_calculations,relevant_periods)}}
    {{metrics.gen_metric_cte(metric,grain,dimensions,secondary_calculations,relevant_periods)}}

{% endmacro %}