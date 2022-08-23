{% macro get_non_calendar_dimension_list(dimensions,calendar_dimensions) %}
    
    {% set calendar_dims = calendar_dimensions %}

    {# Here we set the calendar as either being the default provided by the package
    or the variable provided in the project #}
    {% set dimension_list = [] %}
    {% for dim in dimensions %}
        {%- if dim not in calendar_dimensions -%}
            {%- do dimension_list.append(dim | lower) -%}
        {%- endif -%}
    {% endfor %}
    {%- do return(dimension_list) -%}

{% endmacro %}