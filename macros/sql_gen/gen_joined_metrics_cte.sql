{%- macro gen_joined_metrics_cte(metric_tree, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, metrics_dictionary) -%}
    {{ return(adapter.dispatch('gen_joined_metrics_cte', 'dbt_metrics')(metric_tree, grain, dimensions, calendar_dimensions, secondary_calculations, relevant_periods, metrics_dictionary)) }}
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


, first_join_metrics as (

    select
        date_{{grain}}

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
        , nullif({{metric_name}},0) as {{metric_name}}
    {%- endfor %}  

    from 
        {#- Loop through leaf metric list -#}
        {%- for metric_name in metric_tree.parent_set -%}
            {%- if loop.first %}
        {{ metric_name }}__final
            {%- else %}
        left outer join {{metric_name}}__final 
            using (
                date_{{grain}}
                {%- for calendar_dim in calendar_dimensions %}
                , {{ calendar_dim }}
                {%- endfor %}
                {%- for dim in dimensions %}
                , {{ dim }}
                {%- endfor %}
                )
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
    {%- for metric in metric_tree.ordered_expression_set %}
        {%- if metric_tree.ordered_expression_set[metric] == cte_number %}
        ,({{metrics_dictionary[metric].expression | replace(".metric_value","")}}) as {{metrics_dictionary[metric].name}}
        {%- endif -%}
    {%- endfor %}
    {% if loop.first %}
    from first_join_metrics
    {%- else %}
    from join_metrics__{{previous_cte_number}}
    {%- endif %}


)
    
{%- endfor %}

, joined_metrics as (

    select 
        first_join_metrics.date_{{grain}}
        {%- for period in relevant_periods %}
        ,first_join_metrics.date_{{ period }}
        {%- endfor %}
        {%- for calendar_dim in calendar_dimensions %}
        , first_join_metrics.{{ calendar_dim }}
        {%- endfor %}
        {%- for dim in dimensions %}
        , first_join_metrics.{{ dim }}
        {%- endfor %}
        {%- for metric_name in metric_tree.parent_set %}
        , coalesce(first_join_metrics.{{metric_name}},0) as {{metric_name}}
        {%- endfor %}  
        {%- for metric in metric_tree.ordered_expression_set%}
        , {{metric}}
        {%- endfor %}

    from first_join_metrics
    {% if metric_tree.expression_set | length > 0 %}
    {#- TODO check sort logic -#}
    left join join_metrics__999
        using ( 
            date_{{grain}}
            {%- for calendar_dim in calendar_dimensions %}
            , {{ calendar_dim }}
            {%- endfor %}
            {%- for dim in dimensions %}
            , {{ dim }}
            {%- endfor %}
        )
    {%- endif %}
)

{% endmacro %}