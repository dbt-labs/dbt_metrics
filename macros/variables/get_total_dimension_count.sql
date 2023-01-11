{%- macro get_total_dimension_count(grain, dimensions, calendar_dimensions, relevant_periods) %}

{# This macro calcualtes the total amount of dimensions that will need to be grouped by #}

    {%- set dimension_length = dimensions | length -%}
    {%- set calendar_dimension_length = calendar_dimensions | length -%}

    {%- if grain -%}
        {%- set grain_length = 1 -%}
    {%- else -%}
        {%- set grain_length = 0 -%}
    {%- endif -%}

    {%- set cleaned_relevant_periods = [] -%}
    {%- set period_length = relevant_periods | length -%}
    {%- set total_length = grain_length + dimension_length + period_length + calendar_dimension_length -%}

    {% do return(total_length) %}

{% endmacro %}