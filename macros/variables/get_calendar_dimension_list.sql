{% macro get_calendar_dimension_list(dimensions,dimension_list) %}
    
    {# Here we set the calendar as either being the default provided by the package
    or the variable provided in the project #}
    {% set calendar_dimensions = [] %}
    {% for dim in dimensions %}
        {%- if dim in dimension_list -%}
            {%- do calendar_dimensions.append(dim | lower) -%}
        {%- endif -%}
    {% endfor %}
    {{ log("Metric Name: " ~ metric.name ~ ", Calendar Dimension List: " ~ calendar_dimensions, info=true) }} #}
    {%- do return(calendar_dimensions) -%}

{% endmacro %}