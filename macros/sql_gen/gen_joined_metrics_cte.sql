{%- macro gen_joined_metrics_cte(metric_tree, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, metrics_dictionary) -%}
    {{ return(adapter.dispatch('gen_joined_metrics_cte', 'metrics')(metric_tree, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, metrics_dictionary)) }}
{%- endmacro -%}

{% macro default__gen_joined_metrics_cte(metric_tree, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, metrics_dictionary) %}

{#- This section is a hacky workaround to account for postgres changes -#}
{%- set cte_numbers = [] -%}
{%- set unique_cte_numbers = [] -%}
{#- the cte numbers are more representative of node depth -#}
{%- if metric_tree.expression_set | length > 0 -%}
    {%- for metric_name in metric_tree.ordered_expression_set -%}
        {%- do cte_numbers.append(metric_tree.ordered_expression_set[metric_name]) -%}
    {%- endfor -%}
    {%- for cte_num in cte_numbers|unique -%}
        {%- do unique_cte_numbers.append(cte_num) -%}
    {%- endfor -%}
{%- endif -%}

{% set dimension_count = (dimensions | length + calendar_dimensions | length) | int %}

, first_join_metrics as (

    select
        {% if grain != 'all_time'-%}
        date_{{grain}}
        {%- else -%}
        1 as comma_placeholder
        {%- endif -%}
        {%- for calendar_dim in calendar_dimensions %}
        , coalesce(
            {%- for metric_name in metric_tree.parent_set %}
                {{metric_name}}__final.{{ calendar_dim }}{%- if not loop.last -%},{% endif %}
                {%- if metric_tree.parent_set | length == 1 -%}
                , NULL
                {%- endif -%}
            {% endfor %}
            ) as {{calendar_dim}}
        {% endfor %}

    {%- for period in relevant_periods %}
        , coalesce(
        {%- for metric_name in metric_tree.parent_set %}
            {{metric_name}}__final.date_{{ period }} {%- if not loop.last -%},{% endif %}
            {%- if metric_tree.parent_set | length == 1 %}
            , NULL
            {%- endif -%}
        {% endfor %}
        ) as date_{{period}}
    {%- endfor %}


    {%- for dim in dimensions %}
        , coalesce(
        {%- for metric_name in metric_tree.parent_set %}
            {{metric_name}}__final.{{ dim }} {%- if not loop.last -%},{% endif %}
            {%- if metric_tree.parent_set | length == 1 %}
            , NULL
            {%- endif -%}
        {% endfor %}
        ) as {{dim}}
    {%- endfor %}
    {% for metric_name in metric_tree.parent_set %}
        , {{metric_name}} as {{metric_name}}
    {%- endfor %}  

    {%- if grain == 'all_time' %}
    
        , coalesce(
            {%- for metric_name in metric_tree.parent_set %}
            {{metric_name}}__final.metric_start_date {%- if not loop.last -%},{% endif %}
                {%- if metric_tree.parent_set | length == 1 %}
            , NULL
                {%- endif -%}
            {% endfor %}
        ) as metric_start_date

        , coalesce(
            {%- for metric_name in metric_tree.parent_set %}
            {{metric_name}}__final.metric_end_date {%- if not loop.last -%},{% endif %}
                {%- if metric_tree.parent_set | length == 1 %}
            , NULL
                {%- endif -%}
            {% endfor %}
        ) as metric_end_date

    {%- endif %}

    from 
        {#- Loop through leaf metric list -#}
        {%- for metric_name in metric_tree.parent_set -%}
            {%- if loop.first %}
        {{ metric_name }}__final
            {%- else %}
                {%- if grain != 'all_time'%}
        left outer join {{metric_name}}__final
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
        left outer join {{metric_name}}__final
                    using (
                        {%- for calendar_dim in calendar_dimensions %}
                            {%- if not loop.first -%},{%- endif -%} {{ calendar_dim }}
                        {% endfor -%}
                        
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
        cross join {{metric_name}}__final
                    {%- endif %}
                {%- endif %}
            {%- endif -%}
        {%- endfor %} 
)

{%- for cte_number in cte_numbers | unique | sort %}
    {% set previous_cte_number = cte_number - 1 %}
, join_metrics__{{cte_number}} as (

    select 
    {% if loop.first %}
        first_join_metrics.*
    {%- else %}
        join_metrics__{{previous_cte_number}}.*
    {%- endif %}
    {%- for metric in metric_tree.expression_set %}
        {%- if metric_tree.ordered_expression_set[metric] == cte_number %}
            {#- this logic will parse an expression for divisions signs (/) and wrap all divisors in nullif functions to prevent divide by zero -#}
            {#- "1 / 2 / 3 / ... / N" results in "1 / nullif(2, 0) / nullif(3, 0) / ... / nullif(N, 0)"  -#}
            {%- set metric_expression = metrics_dictionary[metric].expression %}
            {%- if "/" in metric_expression -%}
                {%- set split_division_metric = metric_expression.split('/') -%}
                {%- set dividend = split_division_metric[0] -%}
                {%- set divisors = split_division_metric[1:] | list -%}
                {%- set expression = dividend ~ " / nullif(" ~ divisors | join(", 0) / nullif(") ~ ", 0)" -%}
            {%- else -%}
                {%- set expression = metric_expression -%}
            {%- endif %}
        , ({{ expression | replace(".metric_value","") }}) as {{ metrics_dictionary[metric].name }}
        {%- endif -%}
    {%- endfor -%}

    {% if loop.first %}
    from first_join_metrics
    {%- else %}
    from join_metrics__{{previous_cte_number}}
    {%- endif %}


)
    
{%- endfor %}

, joined_metrics as (

    select 

        {%- if grain != 'all_time' %}
        date_{{grain}}
        {% else %}
        metric_start_date
        , metric_end_date
        {%- endif -%}
        {%- for period in relevant_periods %}
        ,date_{{ period }}
        {%- endfor %}
        {%- for calendar_dim in calendar_dimensions %}
        , {{ calendar_dim }}
        {%- endfor %}
        {%- for dim in dimensions %}
        , {{ dim }}
        {%- endfor %}
        {%- for metric_name in metric_tree.parent_set %}
        , {{metric_name}}
        {%- endfor %}  
        {%- for metric in metric_tree.expression_set %}
        , {{ metric }}
        {% endfor -%}
    
    {% if metric_tree.expression_set | length == 0 %}
    from first_join_metrics
    {% else %}
    from join_metrics__999
    {% endif %}

)

{% endmacro %}