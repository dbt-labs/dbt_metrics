{% macro is_dim_from_model(metric, dim_name) %}
    {% if execute %}
        -- For now, time dimensions have to be encoded in the meta tag. 
        -- If there's no meta config, then assume all dimensions belong to the main model.
        {% if not metric['meta']['dimensions'] %}
            {% do return(True) %}
        {% endif %}

        --TODO: This shouldn't care what order the model/calendar are defined in
        {% set model_dims = metric['meta']['dimensions'][0]['columns'] %}
        {% do return (dim_name in model_dims) %}
    {% else %}
        {% do return (False) %}
    {% endif %}
{% endmacro %}