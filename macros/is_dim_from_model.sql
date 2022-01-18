{% macro is_dim_from_model(metric, dim_name) %}
    {% if execute %}

        {% set model_dims = metric['meta']['dimensions'][0]['columns'] %}
        {% do return (dim_name in model_dims) %}
    {% else %}
        {% do return (False) %}
    {% endif %}
{% endmacro %}