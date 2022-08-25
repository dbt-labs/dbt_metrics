{% macro get_metrics_dictionary(metric_tree) %}

{% set metrics_dictionary = {} %}

{% for metric in metric_tree["full_set"] %}

    {% set dict_metric = metrics.get_metric_relation(metric) %}

    {% do metrics_dictionary.update({metric:{}})%}
    {% do metrics_dictionary[metric].update({'name':dict_metric.name})%}
    {% do metrics_dictionary[metric].update({'type':dict_metric.type})%}
    {% do metrics_dictionary[metric].update({'sql':dict_metric.sql})%}
    {% do metrics_dictionary[metric].update({'timestamp':dict_metric.timestamp})%}
    {% do metrics_dictionary[metric].update({'time_grains':dict_metric.time_grains})%}
    {% do metrics_dictionary[metric].update({'dimensions':dict_metric.dimensions})%}
    {% do metrics_dictionary[metric].update({'filters':dict_metric.filters})%}

    {% if dict_metric.type != 'expression' %}
        {% do metrics_dictionary[metric].update({'metric_model_name':dict_metric.model.replace('"','\'').split('\'')[1]})%}
        {% do metrics_dictionary[metric].update({'metric_model':metrics.get_model_relation(metrics_dictionary[metric]['metric_model_name'], metrics_dictionary[metric]['name'])}) %}
    {% endif %}

{% endfor %}

{% do return(metrics_dictionary) %}

{% endmacro %}