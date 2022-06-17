{% macro is_valid_dimension(dim_name, dimension_list) %}
    {% if execute %}
        {%- if dim_name not in dimension_list -%}
            {%- do exceptions.raise_compiler_error(dim_name ~ " is not a valid dimension") %}
        {%- endif -%}
    {% endif %}
{% endmacro %}