{% macro is_dim_from_model(metric, dim_name) %}
    {% if execute %}
        -- For now, time dimensions have to be encoded in the meta tag. 
        -- If there's no meta config, then assume all dimensions belong to the main model.
        {% if not metric['meta']['dimensions'] %}
            {% do return(True) %}
        {% endif %}

        {% set model_dims = (metric['meta']['dimensions'] | selectattr('type', '==', 'model') | first)['columns'] %}
        {% do return (dim_name in model_dims) %}
    {% else %}
        {% do return (False) %}
    {% endif %}
{% endmacro %}