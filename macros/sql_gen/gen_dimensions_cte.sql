{% macro gen_dimensions_cte(metric,dimensions) %}
    {{ return(adapter.dispatch('gen_dimensions_cte', 'metrics')(metric,dimensions)) }}
{% endmacro %}

{% macro default__gen_dimensions_cte(metric,dimensions) %}

,{{metric.name}}__dims as (
    select distinct
        {% for dim in dimensions %}
            {%- if metrics.is_dim_from_model(metric, dim) -%}
                {{ dim }}
                {% if not loop.last %},{% endif %}
            {% endif -%}
        {%- endfor %}
    from {{metric.name}}__aggregate
)

{% endmacro %}
