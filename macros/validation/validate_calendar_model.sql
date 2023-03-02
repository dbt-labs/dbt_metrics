{% macro validate_calendar_model() %}

    {% set calendar_relation = metrics.get_model_relation(var('dbt_metrics_calendar_model', "dbt_metrics_default_calendar"))%}

{% endmacro %}