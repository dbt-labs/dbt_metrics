{% macro get_joined_models(metric_dictionary, dimensions) %}

    {% set joined_models = [] %}

        {% for dim in dimensions %}
    {% for model_name, dim_list in metric_dictionary.dimensions.items() %}
            {% if dim in dim_list and model_name != metric_dictionary.metric_model.name %}
                {% if model_name not in joined_models %}
                    {% do joined_models.append(model_name) %}
                {% endif %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% do return(joined_models) %}
{% endmacro %}