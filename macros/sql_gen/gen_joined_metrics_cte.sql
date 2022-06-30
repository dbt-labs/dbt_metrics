{% macro gen_joined_metrics_cte(leaf_set,expression_set,grain,dimensions,calendar_dimensions) %}
    {{ return(adapter.dispatch('gen_joined_metrics_cte', 'metrics')(leaf_set,expression_set,grain,dimensions,calendar_dimensions)) }}
{% endmacro %}

{% macro default__gen_joined_metrics_cte(leaf_set,expression_set,grain,dimensions,calendar_dimensions) %}

{# 
Add leaf metric list
Add expression metric list
 #}

    ,joined_metrics as (

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

        {% for metric_name in expression_set %}
            {%- set expression_metric = metrics.get_metric_relation(metric_name) -%}
            {% if loop.first %},{%endif%}
            {{expression_metric.sql | replace(".metric_value","")}} as {{expression_metric.name}}
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

{% endmacro %}
