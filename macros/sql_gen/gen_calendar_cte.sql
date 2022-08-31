{%- macro gen_calendar_cte(calendar_tbl, start_date, end_date) -%}
    {{ return(adapter.dispatch('gen_calendar_cte', 'metrics')(calendar_tbl, start_date, end_date)) }}
{%- endmacro -%}

{%- macro default__gen_calendar_cte(calendar_tbl, start_date, end_date) %}

with calendar as (

    {# This CTE creates our base calendar and then limits the date range for the 
    start and end date provided by the macro call -#}
    select 
        * 
    from {{ calendar_tbl }}
    {% if start_date or end_date %}
        {%- if start_date and end_date -%}
            where date_day >= cast('{{ start_date }}' as date)
            and date_day <= cast('{{ end_date }}' as date)
        {%- elif start_date and not end_date -%}
            where date_day >= cast('{{ start_date }}' as date)
        {%- elif end_date and not start_date -%}
            where date_day <= cast('{{ end_date }}' as date)
        {%- endif -%}       
    {% endif %} 
)

{% endmacro %}
