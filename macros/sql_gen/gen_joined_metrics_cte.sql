{%- macro gen_joined_metrics_cte(metric_tree, metrics_dictionary, models_grouping, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, total_dimension_count) -%}
    {{ return(adapter.dispatch('gen_joined_metrics_cte', 'metrics')(metric_tree, metrics_dictionary, models_grouping, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, total_dimension_count)) }}
{%- endmacro -%}

{% macro default__gen_joined_metrics_cte(metric_tree, metrics_dictionary, models_grouping, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, total_dimension_count) %}

{#- This section is a hacky workaround to account for postgres changes -#}
{%- set cte_numbers = [] -%}
{%- set unique_cte_numbers = [] -%}
{#- the cte numbers are more representative of node depth -#}
{%- if metric_tree.derived_set | length > 0 -%}
    {%- for metric_name in metric_tree.ordered_derived_set -%}
        {%- do cte_numbers.append(metric_tree.ordered_derived_set[metric_name]) -%}
    {%- endfor -%}
    {%- for cte_num in cte_numbers|unique -%}
        {%- do unique_cte_numbers.append(cte_num) -%}
    {%- endfor -%}
{%- endif -%}

{%- set dimension_count = (dimensions | length + calendar_dimensions | length) | int %}
, first_join_metrics as (

    select
        {% if grain -%}
        date_{{grain}},
        {%- endif -%}
        {%- for calendar_dim in calendar_dimensions %}
        coalesce(
        {%- for group_name, group_values in models_grouping.items() %}
                {{group_name}}__final.{{ calendar_dim }}{%- if not loop.last -%},{% endif %}
                {%- if models_grouping | length == 1 -%}
                , NULL
                {%- endif -%}
            {% endfor %}
            ) as {{calendar_dim}},
        {% endfor %}
    {%- for period in relevant_periods %}
        coalesce(
        {%- for group_name, group_values in models_grouping.items() %}
            {{group_name}}__final.date_{{ period }} {%- if not loop.last -%},{% endif %}
            {%- if models_grouping | length == 1 %}
            , NULL
            {%- endif -%}
        {% endfor %}
        ) as date_{{period}},
    {%- endfor %}
    {%- for dim in dimensions %}
        coalesce(
        {%- for group_name, group_values in models_grouping.items() %}
            {{group_name}}__final.{{ dim }} {%- if not loop.last -%},{% endif %}
            {%- if models_grouping | length == 1 %}
            , NULL
            {%- endif -%}
        {% endfor %}
        ) as {{dim}},
    {%- endfor %}

    {%- for metric_name in metric_tree.parent_set %}
        {%- if not metrics_dictionary[metric_name].config.get("treat_null_values_as_zero", True) %}
        {{metric_name}} as {{metric_name}} {%- if not loop.last -%}, {%- endif -%}
        {%- else  %}  
        coalesce({{metric_name}},0) as {{metric_name}} {%- if not loop.last -%}, {%- endif -%}
        {%- endif %}  
    {%- endfor %}  
    {#- Loop through leaf metric list -#}
    {% for group_name, group_values in models_grouping.items() %}
        {%- if loop.first %}
    from {{ group_name }}__final
        {%- else %}
            {%- if grain %}
    full outer join {{group_name}}__final
        using (
            date_{{grain}}
            {%- for calendar_dim in calendar_dimensions %}
            , {{ calendar_dim }}
            {% endfor %}
            {%- for dim in dimensions %}
            , {{ dim }}
            {%- endfor %}
        )
            {%- else -%}
                {% if dimension_count != 0 %}
    full outer join {{group_name}}__final
        using (
            {%- for calendar_dim in calendar_dimensions -%}
                {%- if not loop.first -%},{%- endif -%} {{ calendar_dim }}
            {%- endfor -%}
            
            {%- for dim in dimensions %}
                {%- if loop.first and calendar_dimensions | length == 0 -%}
            {{ dim }}
                {%- elif not loop.first and calendar_dimensions | length == 0 -%}
            , {{ dim }}
                {%- else -%}
            , {{ dim }}
                {%- endif -%}
            {%- endfor -%}
        )
                {%- elif dimension_count == 0 %}
    cross join {{group_name}}__final
                {%- endif %}
            {%- endif %}
        {%- endif -%}
    {%- endfor %} 
{# #}
)

{%- for cte_number in cte_numbers | unique | sort %}
{% set previous_cte_number = cte_number - 1 %}
, join_metrics__{{cte_number}} as (

    select 
    {%- if loop.first %}
        first_join_metrics.*
    {%- else %}
        join_metrics__{{previous_cte_number}}.*
    {%- endif %}
    {%- for metric_name in metric_tree.derived_set %}
        {%- if metric_tree.ordered_derived_set[metric_name] == cte_number %}
            {#- this logic will parse an expression for divisions signs (/) and wrap all divisors in nullif functions to prevent divide by zero -#}
            {#- "1 / 2 / 3 / ... / N" results in "1 / nullif(2, 0) / nullif(3, 0) / ... / nullif(N, 0)"  -#}
            {%- set metric_expression = metrics_dictionary[metric_name].expression %}
            {%- if "/" in metric_expression -%}
                {%- set split_division_metric = metric_expression.split('/') -%}
                {%- set dividend = split_division_metric[0] -%}
                {%- set divisors = split_division_metric[1:] | list -%}
                {%- set expression = dividend ~ " / nullif(" ~ divisors | join(", 0) / nullif(") ~ ", 0)" -%}
            {%- else -%}
                {%- set expression = metric_expression -%}
            {%- endif %}
        , ({{ expression | replace(".metric_value","") }}) as {{ metrics_dictionary[metric_name].name }}
        {%- endif -%}
    {%- endfor -%}
    {% if loop.first %}
    from first_join_metrics
    {%- else %}
    from join_metrics__{{previous_cte_number}}
    {%- endif %}
    {# #}
)
    
{%- endfor %}

, joined_metrics as (

    select 

    {%- if grain %}
        date_{{grain}},
    {%- endif %}

    {%- for period in relevant_periods %}
        date_{{ period }},
    {%- endfor %}

    {%- for calendar_dim in calendar_dimensions %}
        {{ calendar_dim }},
    {%- endfor %}

    {%- for dim in dimensions %}
        {{ dim }},
    {%- endfor %}

    {%- for metric_name in metric_tree.parent_set|list + metric_tree.derived_set|list %}
        {{metric_name}}{%- if not loop.last -%}, {%- endif -%}
    {%- endfor %}  

    {%- if metric_tree.derived_set | length == 0 %}
    from first_join_metrics
    {%- else %}
    from join_metrics__999
    {%- endif %}

)

{% endmacro %}