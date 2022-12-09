{% macro validate_where(where) %}

    {%- if where is iterable and (where is not string and where is not mapping) -%}
        {%- do exceptions.raise_compiler_error("From v0.3.0 onwards, the where clause takes a single string, not a list of filters. Please fix to reflect this change") %}
    {%- endif -%}

{% endmacro %}