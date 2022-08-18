{%- macro build_metric_sql(metric_name, metric_type, metric_sql, metric_timestamp, metric_filters, model, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions, dimensions_provided) %}
    
    {# This is the SQL Gen part - we've broken each component out into individual macros #}
    {# We broke this out so it can loop for composite metrics #}
    {{metrics.gen_aggregate_cte(metric_name, metric_type, metric_sql, metric_timestamp, metric_filters, model, grain, dimensions, secondary_calculations, start_date, end_date, calendar_tbl, relevant_periods, calendar_dimensions)}}
    
    {# Adding conditional logic to exclude the unique combinations of dimensions if there are no dimensions #}
    {% if dimensions_provided == true %}
        {{metrics.gen_dimensions_cte(metric_name, dimensions)}}
    {% endif %}

    {{metrics.gen_spine_time_cte(metric_name, grain, dimensions, secondary_calculations, relevant_periods, calendar_dimensions, dimensions_provided)}}
    {{metrics.gen_metric_cte(metric_name, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions)}}

{% endmacro %}
