{%- macro gen_final_cte(metric_tree, metrics_dictionary, models_grouping, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where, date_alias) -%}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric_tree, metrics_dictionary, models_grouping, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where, date_alias)) }}
{%- endmacro -%}

{%- macro default__gen_final_cte(metric_tree, metrics_dictionary, models_grouping, grain, dimensions, calendar_dimensions, relevant_periods, secondary_calculations, where, date_alias) %}

{%- if secondary_calculations | length > 0 %}
{#- This section is for queries using secondary calculations -#}
select 
    date_{{grain}} {% if date_alias%}as {{date_alias}}{%endif%}
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
    {%- for metric_name in metric_tree.parent_set|list + metric_tree.derived_set|list %}
    ,{{metric_name}}
    {%- endfor %}  
    {{ metrics.gen_secondary_calculations(metric_tree, metrics_dictionary, grain, dimensions, secondary_calculations, calendar_dimensions)}}
    {%- if models_grouping| length > 1 or metric_tree['derived_set'] | length > 0  %}
from joined_metrics 
    {%- else %} 
from {% for group_name, group_values in models_grouping.items()-%}{{group_name}}__final {%-endfor-%}
    {%- endif %}
{# metric where clauses #}
    {%- if where %}
where {{ where }}
    {%- endif %}
{{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}

{%- elif models_grouping| length > 1 or metric_tree['derived_set'] | length > 0 -%}
{#- This section is for queries from multiple models or using derived metrics -#}
select 
    {%- if grain %}
    date_{{grain}} {% if date_alias%}as {{date_alias}}{%endif%},
    {%- endif %}
    {%- for dim in dimensions %}
    {{ dim }},
    {%- endfor %}
    {%- for calendar_dim in calendar_dimensions %}
    {{ calendar_dim }},
    {%- endfor %}
    {%- for metric_name in metric_tree.parent_set|list + metric_tree.derived_set|list %}
    {{metric_name}}{%- if not loop.last -%},{%- endif -%}
    {%- endfor %}  
from joined_metrics
{#- metric where clauses -#}
    {%- if where %}
where {{ where }}
    {%- endif -%}
{{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}
    
{%- else -%}
{#- This section is for non-derived, non-secondary calc queries -#}
select 
    {%- if grain %}
    date_{{grain}} {% if date_alias%}as {{date_alias}}{%endif%},
    {%- endif %}
    {%- for dim in dimensions %}
    {{ dim }},
    {%- endfor %}
    {%- for calendar_dim in calendar_dimensions %}
    {{ calendar_dim }},
    {% endfor -%}
    {%- for metric_name in metric_tree.parent_set|list + metric_tree.derived_set|list %}
    {{metric_name}}{%- if not loop.last -%},{%- endif -%}
    {%- endfor %}  
    {# {%- for metric_name in metric_tree.full_set %}
    {{metric_name}}{%if not loop.last%},{%endif%}
    {%- endfor %} #}
from {% for group_name, group_values in models_grouping.items()-%}{{group_name}}__final {%-endfor-%}
    {%- if where %}
where {{ where }}
    {%- endif -%}
{{ metrics.gen_order_by(grain, dimensions, calendar_dimensions, relevant_periods) }}
{%- endif %}

{%- endmacro %}