{% macro get_metrics_dictionary(metric_tree, develop_yml = none) %}

    {% set metrics_dictionary = {} %}

    {% if develop_yml is not none %}

        {% for metric_name in metric_tree.full_set %}
            {% set metric_definition = develop_yml[metric_name]%}
            {% set single_metric_dict = metrics.get_metric_definition(metric_definition) %}
            {% do metrics_dictionary.update({metric_name:{}})%}
            {% do metrics_dictionary.update({metric_name:single_metric_dict})%}
        {% endfor %}
    
    {% else %}

        {% for metric_name in metric_tree.full_set %}

            {% set dict_metric = metrics.get_metric_relation(metric_name) %}
            {% set single_metric_dict = metrics.get_metric_definition(dict_metric) %}
            {% do metrics_dictionary.update({metric_name:{}})%}
            {% do metrics_dictionary.update({metric_name:single_metric_dict})%}

        {% endfor %}
        
    {% endif %}

    {% do return(metrics_dictionary) %}

{% endmacro %}