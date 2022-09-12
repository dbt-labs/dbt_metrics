{% macro get_metric_model_name(metric_model) %}

    {% set metric_model_name = metric_model.replace('"','\'').split('\'')[1] %}

    {% do return(metric_model_name) %}

{% endmacro %}