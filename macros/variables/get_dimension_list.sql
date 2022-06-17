{% macro get_valid_dimension_list(metric) %}
    
    {# Here we set the calendar as either being the default provided by the package
    or the variable provided in the project #}
    {% set calendar_dims = dbt_utils.get_filtered_columns_in_relation(from=ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar"))) %}

    {% set calendar_dimensions = [] %}
    {% for dim in calendar_dims %}
        {% do calendar_dimensions.append(dim | lower) %}
    {% endfor %}

    {# Here we are going to ensure that the metrics provided are accurate and that they are present 
    in either the metric definition or the default/custom calendar table #}
    {%- set dimension_list = metric.dimensions + calendar_dimensions -%}
    {{ log("Metric Name: " ~ metric.name ~ ", Dimension List: " ~ dimension_list, info=true) }} #}
    {%- do return(dimension_list) -%}

{% endmacro %}