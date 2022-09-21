{% macro get_metrics_dictionary(metric_tree) %}

    {% set metrics_dictionary = {} %}

    {% for metric_name in metric_tree.full_set %}

        {% set dict_metric = metrics.get_metric_relation(metric_name) %}
        {% set single_metric_dict = metrics.get_metric_definition(dict_metric) %}
        {% do metrics_dictionary.update({metric_name:{}})%}
        {% do metrics_dictionary.update({metric_name:single_metric_dict})%}

    {% endfor %}

    {% do return(metrics_dictionary) %}

{% endmacro %}