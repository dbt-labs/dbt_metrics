{% macro get_develop_metrics_dictionary(metric_tree, metric_definition) %}

{% set metrics_dictionary = {} %}

{% for metric_name in metric_tree.full_set %}
    {% do metrics_dictionary.update({metric_definition.name:{}})%}
    {% do metrics_dictionary[metric_name].update({'name': metric_definition.name})%}
    {% do metrics_dictionary[metric_name].update({'type': metric_definition.type})%}
    {% do metrics_dictionary[metric_name].update({'sql': metric_definition.sql})%}
    {% do metrics_dictionary[metric_name].update({'timestamp': metric_definition.timestamp})%}
    {% do metrics_dictionary[metric_name].update({'time_grains': metric_definition.time_grains})%}
    {% do metrics_dictionary[metric_name].update({'dimensions': metric_definition.dimensions})%}
    {% do metrics_dictionary[metric_name].update({'filters': metric_definition.filters})%}
    {% do metrics_dictionary[metric_name].update({'window':metric_definition["window"]}) %}

    {% set metric_model_name = metrics.get_metric_model_name(metric_model=metric_definition.model) %}
    {% do metrics_dictionary[metric_name].update({'metric_model': metrics.get_model_relation(metric_model_name, metric_name)}) %}
    
{% endfor %}

{% do return(metrics_dictionary) %}

{% endmacro %}