{% macro gen_final_cte(metric,grain,metric_list,dimensions) %}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric,grain,metric_list,dimensions)) }}
{% endmacro %}

{% macro default__gen_final_cte(metric,grain,metric_list,dimensions) %}

{%- if metric_list|length > 1 -%}
    ,joined_metrics as (

        select
        date_{{grain}},
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                coalesce(
                {% for metric_name in metric_list %}
                    {{metric_name}}__final.{{ dim }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
                ) as {{dim}},
            {% endif -%}
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
                                {%- if metrics.is_dim_from_model(metric, dim) -%}
                                    {{ dim }}
                                    {% if not loop.last %},{% endif %}
                                {% endif -%}
                            {%- endfor %}
                        )
                {% endif %}
            {% endfor %} 
    )

    select * from joined_metrics
   
{%- else -%}

    select * from {{metric.name}}__final

{%- endif -%}

{% endmacro %}
