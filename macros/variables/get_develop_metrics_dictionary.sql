{% macro get_develop_metrics_dictionary(metric_tree, metric_definition) %}

{% set metrics_dictionary = {} %}

{% for metric in metric_tree["full_set"] %}
    {% do metrics_dictionary.update({metric_definition['name']:{}}) %}
    {% do metrics_dictionary[metric].update({'name':metric_definition["name"]}) %}
    {% do metrics_dictionary[metric].update({'type':metric_definition["type"]}) %}
    {% do metrics_dictionary[metric].update({'sql':metric_definition["sql"]}) %}
    {% do metrics_dictionary[metric].update({'timestamp':metric_definition["timestamp"]}) %}
    {% do metrics_dictionary[metric].update({'time_grains':metric_definition["time_grains"]}) %}
    {% do metrics_dictionary[metric].update({'dimensions':metric_definition["dimensions"]}) %}
    {% do metrics_dictionary[metric].update({'filters':metric_definition["filters"]}) %}
    {% do metrics_dictionary[metric].update({'metric_model_name':metric_definition["model"].replace('"','\'').split('\'')[1]}) %}
    {% do metrics_dictionary[metric].update({'metric_model':metrics.get_model_relation(metrics_dictionary[metric]['metric_model_name'], metrics_dictionary[metric]["name"])}) %}
    {% if metric_definition["meta"] | length != 0 %}
        {% do metrics_dictionary[metric].update({'lookback':metric_definition["meta"]["lookback"]}) %}
    {% endif %}

{% endfor %}

{% do return(metrics_dictionary) %}

{% endmacro %}