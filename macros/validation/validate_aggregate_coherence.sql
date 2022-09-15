{% macro validate_aggregate_coherence(metric_aggregate, calculation_aggregate) %}
    {% set allowlist = metrics.get_metric_allowlist()[metric_aggregate] %}

    {% if (calculation_aggregate not in allowlist) %}
        {% do exceptions.raise_compiler_error("Can't calculate secondary aggregate " ~ calculation_aggregate ~ " when metric's aggregation is " ~ metric_aggregate ~ ". Allowed options are " ~ allowlist ~ ".") %}
    {% endif %}
{% endmacro %}