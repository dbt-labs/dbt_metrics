{% macro get_calendar_dimension_list(dimensions,dimension_list) %}
    
    {% set calendar_dims = dbt_utils.get_filtered_columns_in_relation(from=ref(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar"))) %}
    {% set calendar_dimensions = [] %}
    {% for dim in calendar_dims %}
        {% do calendar_dimensions.append(dim | lower) %}
    {% endfor %}

    {# Here we set the calendar as either being the default provided by the package
    or the variable provided in the project #}
    {% set approved_calendar_dimensions = [] %}
    {% for dim in dimensions %}
        {%- if dim in calendar_dimensions -%}
            {%- do approved_calendar_dimensions.append(dim | lower) -%}
        {%- endif -%}
    {% endfor %}
    {%- do return(approved_calendar_dimensions) -%}

{% endmacro %}