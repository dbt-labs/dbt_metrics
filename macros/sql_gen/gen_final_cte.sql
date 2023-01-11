{%- macro gen_final_cte(metric_tree, metrics_dictionary, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where, date_alias) -%}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric_tree, metrics_dictionary, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where, date_alias)) }}
{%- endmacro -%}

{%- macro default__gen_final_cte(metric_tree, metrics_dictionary, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where, date_alias) %}
{%- if secondary_calculations | length > 0 %}
select 
    date_{{grain}} {% if date_alias%}as {{date_alias}} {%endif%}
    {%- if secondary_calculations | length > 0 -%}
        {%- for period in relevant_periods %}
    ,date_{{ period }}
        {%- endfor %}
    {%- endif -%}
    {%- for dim in dimensions %}
    ,{{ dim }}
    {%- endfor %}
    {%- for calendar_dim in calendar_dimensions %}
    ,{{ calendar_dim }}
    {%- endfor %}
    {%- for metric_name in metric_tree.full_set %}
    ,{{metric_name}}
    {%- endfor %}
    {{ metrics.gen_secondary_calculations(metric_tree, metrics_dictionary, grain, dimensions, secondary_calculations, calendar_dimensions)}}
from {% if metric_tree.full_set | length > 1 -%} joined_metrics {%- else -%} {{ metric_tree.base_set[0] }}__final {%- endif %}
{# metric where clauses #}
{%- if where %}
where {{ where }}
{%- endif %}
{{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}

{%- else %}

{%- if metric_tree.full_set | length > 1 %}
select 
    {%- if grain %}
    date_{{grain}} {% if date_alias%}as {{date_alias}} {%endif%},
    {% endif -%}
    {%- for dim in dimensions %}
    {{ dim }},
    {%- endfor %}
    {%- for calendar_dim in calendar_dimensions %}
    {{ calendar_dim }},
    {% endfor -%}
    {%- for metric_name in metric_tree.full_set %}
    {{metric_name}}{%if not loop.last%},{%endif%}
    {% endfor -%}
from joined_metrics
{#- metric where clauses -#}
{%- if where %}
where {{ where }}
{%- endif -%}
{{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}
    
{%- else %}

select 
    {%- if grain %}
    date_{{grain}} {% if date_alias%}as {{date_alias}} {%endif%},
    {%- endif %}
    {%- for dim in dimensions %}
    {{ dim }},
    {%- endfor %}
    {%- for calendar_dim in calendar_dimensions %}
    {{ calendar_dim }},
    {% endfor -%}
    {%- for metric_name in metric_tree.full_set %}
    {{metric_name}}{%if not loop.last%},{%endif%}
    {%- endfor %}
from {{metric_tree.base_set[0]}}__final 
    {%- if where %}
where {{ where }}
    {%- endif -%}
{{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}
    {%- endif %}
{%- endif %}

{%- endmacro %}