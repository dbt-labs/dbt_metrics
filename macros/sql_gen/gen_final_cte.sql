{% macro gen_final_cte(metric,metric_list,dimensions) %}
    {{ return(adapter.dispatch('gen_final_cte', 'metrics')(metric,metric_list,dimensions)) }}
{% endmacro %}

{% macro default__gen_final_cte(metric,metric_list,dimensions) %}

    ,joined_metrics as (

        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                coalesce(
                {% for metric in metric_list %}
                    {{metric}}__joined.{{ dim }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
                ) as {{dim}},
            {% endif -%}
        {%- endfor %}
        {% for metric in metric_list %}
            {{metric}},
        {% endfor %}   

        from 
            {% for metric in metric_list %}
                {{metric}}__joined
                {% if not loop.first %}
                    left outer join {{metric}}__joined 
                        using (
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
   

{% endmacro %}
