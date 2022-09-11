{% macro get_metrics_dictionary(metric_tree) %}

{% set metrics_dictionary = {} %}

{% for metric_name in metric_tree.full_set %}

    {% set dict_metric = dbt_metrics.get_metric_relation(metric_name) %}

    {% do metrics_dictionary.update({metric_name:{}})%}
    {% do metrics_dictionary[metric_name].update({'name': dict_metric.name})%}
    {% do metrics_dictionary[metric_name].update({'calculation_method': dict_metric.calculation_method})%}
    {% do metrics_dictionary[metric_name].update({'expression': dict_metric.expression})%} 
    {% do metrics_dictionary[metric_name].update({'timestamp': dict_metric.timestamp})%}
    {% do metrics_dictionary[metric_name].update({'time_grains': dict_metric.time_grains})%}
    {% do metrics_dictionary[metric_name].update({'dimensions': dict_metric.dimensions})%}
    {% do metrics_dictionary[metric_name].update({'filters': dict_metric.filters})%}

    {% if dict_metric.calculation_method != 'derived' %}
        {% set metric_model_name = dbt_metrics.get_metric_model_name(metric_model=dict_metric.model) %}
        {% do metrics_dictionary[metric_name].update({'metric_model': dbt_metrics.get_model_relation(metric_model_name, metric_name)}) %}
    {% endif %}

{% endfor %}

{% do return(metrics_dictionary) %}

{% endmacro %}