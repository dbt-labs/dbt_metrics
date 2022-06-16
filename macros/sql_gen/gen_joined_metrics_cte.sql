{% macro gen_joined_metrics_cte(metric,grain,dimensions,metric_list) %}
    {{ return(adapter.dispatch('gen_joined_metrics_cte', 'metrics')(metric,grain,dimensions,metric_list)) }}
{% endmacro %}

{% macro default__gen_joined_metrics_cte(metric,grain,dimensions,metric_list) %}

    ,joined_metrics as (

        select
        date_{{grain}},
        {% for dim in dimensions %}
            coalesce(
            {% for metric_name in metric_list %}
                {{metric_name}}__final.{{ dim }}
                {% if not loop.last %},{% endif %}
            {% endfor %}
            ) as {{dim}},
        {%- endfor %}
        {% for metric_name in metric_list %}
            {{metric_name}},
        {% endfor %}  

        ({{metric.sql | replace(".metric_value","")}}) as {{metric.name}}

        from 
            {% for metric_name in metric_list %}
                {% if loop.first %}
                    {{metric_name}}__final
                {% else %}
                    left outer join {{metric_name}}__final 
                        using ( date_{{grain}},
                            {% for dim in dimensions %}
                                {{ dim }}
                                {% if not loop.last %},{% endif %}
                            {%- endfor %}
                        )
                {% endif %}
            {% endfor %} 
    )

{% endmacro %}
