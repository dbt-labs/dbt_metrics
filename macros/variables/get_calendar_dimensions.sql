{% macro get_calendar_dimensions(dimensions) %}
    
    {% set approved_calendar_dimensions = var('custom_calendar_dimension_list',[]) %}

    {# Here we set the calendar as either being the default provided by the package
    or the variable provided in the project #}
    {% set calendar_dimensions = [] %}
    {% for dim in dimensions %}
        {%- if dim in approved_calendar_dimensions -%}
            {%- do calendar_dimensions.append(dim | lower) -%}
        {%- endif -%}
    {% endfor %}
    {%- do return(calendar_dimensions) -%}

{% endmacro %}