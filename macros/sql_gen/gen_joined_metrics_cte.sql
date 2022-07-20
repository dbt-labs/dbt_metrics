{% macro gen_joined_metrics_cte(leaf_set,expression_set,ordered_expression_set,grain,dimensions,calendar_dimensions,secondary_calculations,relevant_periods) %}
    {{ return(adapter.dispatch('gen_joined_metrics_cte', 'metrics')(leaf_set,expression_set,ordered_expression_set,grain,dimensions,calendar_dimensions,secondary_calculations,relevant_periods)) }}
{% endmacro %}

{% macro default__gen_joined_metrics_cte(leaf_set,expression_set,ordered_expression_set,grain,dimensions,calendar_dimensions,secondary_calculations,relevant_periods) %}

{# This section is a hacky workaround to account for postgres changes #}
{% set cte_numbers = []%}
{% set unique_cte_numbers = []%}
{# the cte numbers are more representative of node depth #}
{% if expression_set | length > 0 %}
    {% for metric in ordered_expression_set%}
        {% do cte_numbers.append(ordered_expression_set[metric]) %}
    {% endfor %}
    {% for cte_num in cte_numbers|unique%}
        {% do unique_cte_numbers.append(cte_num) %}
    {% endfor %}
{% endif %}


    ,first_join_metrics as (

        select
        date_{{grain}},

        {% for calendar_dim in calendar_dimensions %}
            coalesce(
            {% for metric_name in leaf_set %}
                {{metric_name}}__final.{{ calendar_dim }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
            ) as {{calendar_dim}},
        {%- endfor %}

        {% if secondary_calculations | length > 0 %}
            {% for period in relevant_periods %}
                coalesce(
                {% for metric_name in leaf_set %}
                    {{metric_name}}__final.date_{{ period }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
                ) as date_{{period}},
            {% endfor %}
        {% endif %}


        {% for dim in dimensions %}
            coalesce(
            {% for metric_name in leaf_set %}
                {{metric_name}}__final.{{ dim }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
            ) as {{dim}},
        {%- endfor %}

        {% for metric_name in leaf_set %}
            {{metric_name}}
            {% if not loop.last %},{%endif%}
        {% endfor %}  

        from 
            {# Loop through leaf metric list #}
            {% for metric_name in leaf_set %}
                {% if loop.first %}
                    {{metric_name}}__final
                {% else %}
                    left outer join {{metric_name}}__final 
                        using ( date_{{grain}},
                            {% for calendar_dim in calendar_dimensions %}
                                {{ calendar_dim }},
                            {%- endfor %}
                            {% for dim in dimensions %}
                                {{ dim }}
                                {% if not loop.last %},{% endif %}
                            {%- endfor %}
                        )
                {% endif %}
            {% endfor %} 
    )
    ,

    {% for cte_number in cte_numbers|unique|sort%}
        {% set previous_cte_number = cte_number - 1%}
        "{{cte_number}}__join_metrics" as (

            select 
            {% if loop.first %}
                first_join_metrics.*
            {% else %}
                "{{previous_cte_number}}__join_metrics".*
            {%endif%}
                {% for metric in ordered_expression_set%}
                    {% if ordered_expression_set[metric] == cte_number%}
                        {%- set expression_metric = metrics.get_metric_relation(metric) -%}
                        ,({{expression_metric.sql | replace(".metric_value","")}}) as {{expression_metric.name}}
                    {% endif %}
                {% endfor %}
            {% if loop.first %}
                from first_join_metrics
            {% else %}
                from "{{previous_cte_number}}__join_metrics"
            {%endif%}


            )
        ,
    {% endfor %}
    joined_metrics as (

        select 
            first_join_metrics.*
            {% for metric in ordered_expression_set%}
                {% if loop.first %},{%endif%}
                {{metric}}
                {% if not loop.last %},{%endif%}
            {% endfor %}
        from first_join_metrics
        {% if expression_set | length > 0 %}
        {# TODO check sort logic #}
            left join "999__join_metrics"
                using ( date_{{grain}},
                    {% for calendar_dim in calendar_dimensions %}
                        {{ calendar_dim }},
                    {%- endfor %}
                    {% for dim in dimensions %}
                        {{ dim }}
                        {% if not loop.last %},{% endif %}
                    {%- endfor %}
                )
        {% endif %}

    )

{% endmacro %}
