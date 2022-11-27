{%- macro gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) -%}
    {{ return(adapter.dispatch('gen_order_by', 'metrics')(grain, dimensions, calendar_dimensions, relevant_periods)) }}
{%- endmacro -%}

{% macro default__gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) %}

{# This macro has gone through many revisions. As of 1.3.2, it will be changed to reduce its 
functionality to only order by the date field #}

    {% if grain != 'all_time' %}
    order by 1 desc
    {% endif %}

{% endmacro %}