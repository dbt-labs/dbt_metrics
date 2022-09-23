{%- macro gen_metric_cte(metric_name, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, treat_null_values_as_zero) -%}
    {{ return(adapter.dispatch('gen_metric_cte', 'metrics')(metric_name, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, treat_null_values_as_zero)) }}
{%- endmacro -%}

{%- macro default__gen_metric_cte(metric_name, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions, treat_null_values_as_zero) %}

, {{metric_name}}__final as (

    {%- if not treat_null_values_as_zero -%}
        {%- set metric_val = metric_name -%}
    {%- else -%}
        {%- set metric_val = "coalesce(" ~ metric_name ~ ", 0) as " ~ metric_name -%}
    {%- endif %}
    
    select
        {% if grain != 'all_time' %}
        parent_metric_cte.date_{{grain}},
            {%- if secondary_calculations | length > 0 -%}
                {% for period in relevant_periods %}
        parent_metric_cte.date_{{ period }},
                {%- endfor -%}
            {%- endif -%}
        {%- endif -%}
        
        {%- for calendar_dim in calendar_dimensions %}
        parent_metric_cte.{{ calendar_dim }},
        {%- endfor %}

        {%- for dim in dimensions %}
        parent_metric_cte.{{ dim }},
        {%- endfor %}
        {{ metric_val }}
        
    {%- if grain == 'all_time' %}

        ,metric_start_date
        ,metric_end_date

    from {{metric_name}}__aggregate as parent_metric_cte

    {% else %}

    from {{metric_name}}__spine_time as parent_metric_cte
    left outer join {{metric_name}}__aggregate
        using (
            date_{{grain}}
            {%- for calendar_dim in calendar_dimensions %}
            , {{ calendar_dim }}
            {%- endfor %}
            {%- for dim in dimensions %}
            , {{ dim }}
            {%- endfor %}
        )

        {% if not start_date or not end_date -%}
        where (
            {% if not start_date and not end_date -%}
            parent_metric_cte.date_{{grain}} >= (
                select 
                    min(case when has_data then date_{{grain}} end) 
                from {{metric_name}}__aggregate
            )
            and parent_metric_cte.date_{{grain}} <= (
                select 
                    max(case when has_data then date_{{grain}} end) 
                from {{metric_name}}__aggregate
            )
            {% elif not start_date and end_date -%}
            parent_metric_cte.date_{{grain}} >= (
                select 
                    min(case when has_data then date_{{grain}} end) 
                from {{metric_name}}__aggregate
            )
            {% elif start_date and not end_date -%}
            parent_metric_cte.date_{{grain}} <= (
                select 
                    max(case when has_data then date_{{grain}} end) 
                from {{metric_name}}__aggregate
            )
            {%- endif %} 
        )      
        {% endif %} 
    {% endif -%}

)

{% endmacro %}
