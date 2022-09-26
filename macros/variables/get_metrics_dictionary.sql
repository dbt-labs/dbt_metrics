{% macro get_metrics_dictionary(metric_tree, develop_yml = none) %}

    {% set metrics_dictionary = {} %}

    {% for metric_name in metric_tree.full_set %}
        {% if develop_yml is not none %}
            {% set metric_object = develop_yml[metric_name]%}
        {% else %}
            {% set metric_object = metrics.get_metric_relation(metric_name) %}
        {% endif %}
        {% set metric_definition = metrics.get_metric_definition(metric_object) %}
        {% if not metric_definition.config %}
            {% do metric_definition.update({'config':{}}) %}
        {% endif %}
        {% do metrics_dictionary.update({metric_name:{}})%}
        {% do metrics_dictionary.update({metric_name:metric_definition})%}
    {% endfor %}

    {% do return(metrics_dictionary) %}

{% endmacro %}