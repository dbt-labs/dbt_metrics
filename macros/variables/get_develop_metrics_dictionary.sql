{% macro get_develop_metrics_dictionary(metric_tree, develop_yml) %}

    {% set metrics_dictionary = {} %}

    {% for metric_name in metric_tree.full_set %}
        {% set metric_definition = develop_yml[metric_name]%}
        {% set single_metric_dict = metrics.get_metric_definition(metric_definition) %}
        {% do metrics_dictionary.update({metric_name:{}})%}
        {% do metrics_dictionary.update({metric_name:single_metric_dict})%}
    {% endfor %}

    {% do return(metrics_dictionary) %}

{% endmacro %}