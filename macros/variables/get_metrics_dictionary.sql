{% macro get_metrics_dictionary(metric_tree) %}

{% set metrics_dictionary = {} %}

{% for metric_name in metric_tree["full_set"] %}

    {% set dict_metric = metrics.get_metric_relation(metric_name) %}

    {% do metrics_dictionary.update({metric_name:{}})%}
    {% do metrics_dictionary[metric_name].update({'name': dict_metric.name})%}
    {% do metrics_dictionary[metric_name].update({'type': dict_metric.type})%}
    {% do metrics_dictionary[metric_name].update({'sql': dict_metric.sql})%} 
    {% do metrics_dictionary[metric_name].update({'timestamp': dict_metric.timestamp})%}
    {% do metrics_dictionary[metric_name].update({'time_grains': dict_metric.time_grains})%}
    {% do metrics_dictionary[metric_name].update({'dimensions': dict_metric.dimensions})%}
    {% do metrics_dictionary[metric_name].update({'filters': dict_metric.filters})%}

    {% if dict_metric.type != 'expression' %}
        {% set metric_model_name = metrics.get_metric_model_name(metric_model=dict_metric.model) %}
        {% do metrics_dictionary[metric_name].update({'metric_model': metrics.get_model_relation(metric_model_name)}) %}
    {% endif %}

{% endfor %}

{% do return(metrics_dictionary) %}

{% endmacro %}