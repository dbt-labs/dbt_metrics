{% macro is_valid_dimension(metric, dim_name, calendar_dimensions) %}
    {% if execute %}

        {% set model_dims = metric.dimensions %}

        {% set total_dims = model_dims + calendar_dimensions %}
        {%- if dim_name in total_dims -%}
            {%- do return(dim_name) -%}
        {% else %}
            {%- do exceptions.raise_compiler_error(dim_name ~ " is not a valid dimension") %}
        {%- endif -%}
    {% endif %}
{% endmacro %}