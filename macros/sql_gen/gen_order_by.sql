{%- macro gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) -%}
    {{ return(adapter.dispatch('gen_order_by', 'metrics')(grain, dimensions, calendar_dimensions, relevant_periods)) }}
{%- endmacro -%}

{% macro default__gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) %}
    {# #}
    {%- if grain %}
order by 1 desc
    {% endif -%}
    {# #}
{% endmacro %}