{%- macro gen_group_by(grain, dimensions, calendar_dimensions, relevant_periods) -%}
    {{ return(adapter.dispatch('gen_group_by', 'dbt_metrics')(grain, dimensions, calendar_dimensions, relevant_periods)) }}
{%- endmacro -%}

{% macro default__gen_group_by(grain, dimensions, calendar_dimensions, relevant_periods) %}

{#- This model exclusively exists because dynamic group by counts based on range 
were too funky when we hardcoded values for 1+1. So we're getting around it by
making it its own function -#}

{#- The issue arises when we have an initial date column (ie date_month) where month 
is also included in the relevent periods. This causes issues and so we need to
remove the grain from the list of relevant periods so it isnt double counted -#}

    {%- set dimension_length = dimensions | length -%}
    {%- set calendar_dimension_length = calendar_dimensions | length -%}

    {%- set cleaned_relevant_periods = [] -%}
    {%- set period_length = relevant_periods | length -%}
    {%- set total_length = dimension_length + period_length + calendar_dimension_length -%}

    {% if grain == 'all_time' %}
        {% if total_length > 0%}
            group by
            {% for number in range(1,total_length+1) -%}
                {{ number }} {%- if not loop.last -%}, {% endif -%}
            {% endfor -%}
        {% endif %}
    {% else %}
        group by
        {% for number in range(1,total_length+2) -%}
            {{ number }} {%- if not loop.last -%}, {% endif -%}
        {% endfor -%}
    {% endif %}

{% endmacro %}
