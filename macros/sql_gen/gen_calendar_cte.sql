{%- macro gen_calendar_cte(calendar_tbl, start_date, end_date, grain) -%}
    {{ return(adapter.dispatch('gen_calendar_cte', 'metrics')(calendar_tbl, start_date, end_date, grain)) }}
{%- endmacro -%}

{%- macro default__gen_calendar_cte(calendar_tbl, start_date, end_date, grain) %}

with calendar as (
    {# This CTE creates our base calendar and then limits the date range for the 
    start and end date provided by the macro call -#}
    select 
    {% if grain == '15min' %}
        to_timestamp_ntz(concat(date_day, ' ', hour, ':', minute), 'YYYY-MM-DD HH24:MI') as date_15min,
    {% elif grain == 'hour' %}
        to_timestamp_ntz(concat(date_day, ' ', hour), 'YYYY-MM-DD HH24') as date_hour,
    {% endif %}
        c.* 
    from {{ calendar_tbl }} c
    {% if grain == 'hour' or grain == '15min'%}
        cross join
        (
        values
            ('00'),
            ('01'),
            ('02'),
            ('03'),
            ('04'),
            ('05'),
            ('06'),
            ('07'),
            ('08'),
            ('09'),
            ('10'),
            ('11'),
            ('12'),
            ('13'),
            ('14'),
            ('15'),
            ('16'),
            ('17'),
            ('18'),
            ('19'),
            ('20'),
            ('21'),
            ('22'),
            ('23')
    ) hours(hour)
    {% endif %}
    {% if grain == '15min'%}
        cross join
        (
        values
            ('00'),
            ('15'),
            ('30'),
            ('45')
    ) minutes(minute)
    {% endif %}
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

{%- endmacro -%}
