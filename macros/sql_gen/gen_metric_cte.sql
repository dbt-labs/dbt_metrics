{%- macro gen_metric_cte(metrics_dictionary, group_name, group_values, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions) -%}
    {{ return(adapter.dispatch('gen_metric_cte', 'metrics')(metrics_dictionary, group_name, group_values, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions)) }}
{%- endmacro -%}

{%- macro default__gen_metric_cte(metrics_dictionary, group_name, group_values, grain, dimensions, secondary_calculations, start_date, end_date, relevant_periods, calendar_dimensions) %}

{%- set combined_dimensions = calendar_dimensions | list + dimensions | list -%}
, {{group_name}}__final as (
    {# #}
    select
        {%- if grain %}
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

        {%- for metric_name in group_values.metric_names -%}
            {# TODO: coalesce based on the value. Need to bring this config #}
            {%- if not metrics_dictionary[metric_name].get("config").get("treat_null_values_as_zero", True) %}
        {{ metric_name }}
            {%- else %}
        coalesce({{ metric_name }}, 0) as {{ metric_name }}
            {%- endif %}
        {%- if not loop.last-%},{%endif%}
        {%- endfor %}

    {%- if secondary_calculations | length > 0 %}
    from {{group_name}}__spine_time as parent_metric_cte
    left outer join {{group_name}}__aggregate
        using (date_{{grain}} {%- if combined_dimensions | length > 0 -%}, {{ combined_dimensions | join(", ") }} {%-endif-%} )

    {% if not start_date or not end_date -%}
    where (
        {% if not start_date and not end_date -%}
        parent_metric_cte.date_{{grain}} >= (
            select 
                min(case when has_data then date_{{grain}} end) 
            from {{group_name}}__aggregate
        )
        and parent_metric_cte.date_{{grain}} <= (
            select 
                max(case when has_data then date_{{grain}} end) 
            from {{group_name}}__aggregate
        )
        {% elif not start_date and end_date -%}
        parent_metric_cte.date_{{grain}} >= (
            select 
                min(case when has_data then date_{{grain}} end) 
            from {{group_name}}__aggregate
        )
        {% elif start_date and not end_date -%}
        parent_metric_cte.date_{{grain}} <= (
            select 
                max(case when has_data then date_{{grain}} end) 
            from {{group_name}}__aggregate
        )
        {%- endif %} 
        )
    {%- endif %} 

    {%- else %}
    from {{group_name}}__aggregate as parent_metric_cte
    {%- endif %}
)

{% endmacro %}
